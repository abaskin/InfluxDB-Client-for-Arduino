/**
 *
 * InfluxDBClient.cpp: InfluxDB Client for Arduino
 *
 * MIT License
 *
 * Copyright (c) 2020 InfluxData
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#include "InfluxDbClient.h"

#include "Platform.h"
#include "Version.h"

constexpr auto TooEarlyMessage =
    "Cannot send request yet because of applied retry strategy. Remaining ";

static std::string escapeJSONString(const std::string &value);
static std::string precisionToString(WritePrecision precision,
                                     uint8_t version = 2) {
  switch (precision) {
    case WritePrecision::US:
      return version == 1 ? "u" : "us";
    case WritePrecision::MS:
      return "ms";
    case WritePrecision::NS:
      return "ns";
    case WritePrecision::S:
      return "s";
    default:
      return "";
  }
}

InfluxDBClient::InfluxDBClient() : _writeBuffer(new Batch{}) {}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl,
                               const std::string &db)
    : InfluxDBClient() {
  setConnectionParamsV1(serverUrl, db);
}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl,
                               const std::string &org,
                               const std::string &bucket,
                               const std::string &authToken)
    : InfluxDBClient(serverUrl, org, bucket, authToken, nullptr) {}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl,
                               const std::string &org,
                               const std::string &bucket,
                               const std::string &authToken,
                               const char *serverCert)
    : InfluxDBClient() {
  setConnectionParams(serverUrl, org, bucket, authToken, serverCert);
}

void InfluxDBClient::setInsecure(bool value) { _connInfo.insecure = value; }

void InfluxDBClient::setConnectionParams(const std::string &serverUrl,
                                         const std::string &org,
                                         const std::string &bucket,
                                         const std::string &authToken,
                                         const char *certInfo) {
  clean();
  _connInfo.serverUrl = serverUrl;
  _connInfo.bucket = bucket;
  _connInfo.org = org;
  _connInfo.authToken = authToken;
  _connInfo.certInfo = certInfo;
  _connInfo.dbVersion = 2;
}

void InfluxDBClient::setConnectionParamsV1(const std::string &serverUrl,
                                           const std::string &db,
                                           const std::string &user,
                                           const std::string &password,
                                           const char *certInfo) {
  clean();
  _connInfo.serverUrl = serverUrl;
  _connInfo.bucket = db;
  _connInfo.user = user;
  _connInfo.password = password;
  _connInfo.certInfo = certInfo;
  _connInfo.dbVersion = 1;
}

bool InfluxDBClient::init() {
  INFLUXDB_CLIENT_DEBUG("[D] Init\n");
  INFLUXDB_CLIENT_DEBUG("[D]  Library version: " INFLUXDB_CLIENT_VERSION "\n");
  INFLUXDB_CLIENT_DEBUG("[D]  Device : " INFLUXDB_CLIENT_PLATFORM "\n");
  INFLUXDB_CLIENT_DEBUG("[D]  SDK version: " INFLUXDB_CLIENT_PLATFORM_VERSION
                        "\n");
  INFLUXDB_CLIENT_DEBUG("[D]  Server url: %s\n", _connInfo.serverUrl.c_str());
  INFLUXDB_CLIENT_DEBUG("[D]  Org: %s\n", _connInfo.org.c_str());
  INFLUXDB_CLIENT_DEBUG("[D]  Bucket: %s\n", _connInfo.bucket.c_str());
  INFLUXDB_CLIENT_DEBUG("[D]  Token: %s\n", _connInfo.authToken.c_str());
  INFLUXDB_CLIENT_DEBUG("[D]  DB version: %d\n", _connInfo.dbVersion);
  if (_connInfo.serverUrl.length() == 0 ||
      (_connInfo.dbVersion == 2 &&
       (_connInfo.org.length() == 0 || _connInfo.bucket.length() == 0 ||
        _connInfo.authToken.length() == 0))) {
    INFLUXDB_CLIENT_DEBUG("[E] Invalid parameters\n");
    _connInfo.lastError = "Invalid parameters";
    return false;
  }
  if (endsWith(_connInfo.serverUrl, "/")) {
    _connInfo.serverUrl =
        _connInfo.serverUrl.substr(0, _connInfo.serverUrl.length() - 1);
  }
  if (!startsWith(_connInfo.serverUrl, "http")) {
    _connInfo.lastError = "Invalid URL scheme";
    return false;
  }
  _service.reset(new HTTPService(&_connInfo));

  setUrls();

  return true;
}

InfluxDBClient::~InfluxDBClient() { clean(); }

void InfluxDBClient::clean() { _buckets.reset(nullptr); }

bool InfluxDBClient::setUrls() {
  if (!_service && !init()) {
    return false;
  }
  INFLUXDB_CLIENT_DEBUG("[D] setUrls\n");
  if (_connInfo.dbVersion == 2) {
    _writeUrl = _service->getServerAPIURL();
    _writeUrl += "write?org=";
    _writeUrl += urlEncode(_connInfo.org.c_str());
    _writeUrl += "&bucket=";
    _writeUrl += urlEncode(_connInfo.bucket.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
    _queryUrl = _service->getServerAPIURL();
    ;
    _queryUrl += "query?org=";
    _queryUrl += urlEncode(_connInfo.org.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  queryUrl: %s\n", _queryUrl.c_str());
  } else {
    _writeUrl = _connInfo.serverUrl;
    _writeUrl += "/write?db=";
    _writeUrl += urlEncode(_connInfo.bucket.c_str());
    _queryUrl = _connInfo.serverUrl;
    _queryUrl += "/api/v2/query";
    if (_connInfo.user.length() > 0 && _connInfo.password.length() > 0) {
      std::string auth = "&u=";
      auth += urlEncode(_connInfo.user.c_str());
      auth += "&p=";
      auth += urlEncode(_connInfo.password.c_str());
      _writeUrl += auth;
      _queryUrl += "?";
      _queryUrl += auth;
    }
    // on version 1.x /ping will by default return status code 204, without
    // verbose
    _validateUrl =
        _connInfo.serverUrl +
        (_connInfo.dbVersion == 2 ? "/health" : "/ping?verbose=true");
    if (_connInfo.dbVersion == 1 && _connInfo.user.length() > 0 &&
        _connInfo.password.length() > 0) {
      _validateUrl.append("&u=");
      _validateUrl.append(urlEncode(_connInfo.user.c_str()));
      _validateUrl.append("&p=");
      _validateUrl.append(urlEncode(_connInfo.password.c_str()));
    }
    INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  queryUrl: %s\n", _queryUrl.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  validateUrl: %s\n", _validateUrl.c_str());
  }
  if (_writeOptions._writePrecision != WritePrecision::NoTime) {
    _writeUrl += "&precision=";
    _writeUrl +=
        precisionToString(_writeOptions._writePrecision, _connInfo.dbVersion);
    INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
  }
  return true;
}

bool InfluxDBClient::setWriteOptions(WritePrecision precision,
                                     uint16_t batchSize, uint16_t bufferSize,
                                     std::chrono::seconds flushInterval,
                                     bool preserveConnection) {
  if (!_service && !init()) {
    return false;
  }
  if (!setWriteOptions(WriteOptions()
                           .writePrecision(precision)
                           .batchSize(batchSize)
                           .bufferSize(bufferSize)
                           .flushInterval(flushInterval))) {
    return false;
  }
  if (!setHTTPOptions(
          _service->getHTTPOptions().connectionReuse(preserveConnection))) {
    return false;
  }
  return true;
}

bool InfluxDBClient::setWriteOptions(const WriteOptions &writeOptions) {
  if (_writeOptions._writePrecision != writeOptions._writePrecision) {
    _writeOptions._writePrecision = writeOptions._writePrecision;
    if (!setUrls()) {
      return false;
    }
  }

  if (writeOptions._batchSize > 0 &&
      _writeOptions._batchSize != writeOptions._batchSize) {
    _writeOptions._batchSize = writeOptions._batchSize;
    INFLUXDB_CLIENT_DEBUG("[D] Changing batch size to %d\n",
                          _writeOptions._batchSize);
  }

  if (writeOptions._bufferSize > 0 &&
      _writeOptions._bufferSize != writeOptions._bufferSize) {
    _writeOptions._bufferSize = writeOptions._bufferSize;
    _writeBuffer->setBufferSize(_writeOptions._batchSize *
                                _writeOptions._bufferSize);
    INFLUXDB_CLIENT_DEBUG("[D] Changing buffer size to %d\n",
                          _writeOptions._bufferSize);
  }

  _writeOptions._flushInterval = writeOptions._flushInterval;
  if (_writeOptions._flushInterval.count() > 0) {
    _flushTicker.attach_ms(
        _writeOptions._flushInterval.count() * 1000,
        +[](InfluxDBClient *c) {
          c->_writeBuffer->_write = true;
          INFLUXDB_CLIENT_DEBUG(
              "[D] Reached write flush interval, marked for writing\n");
        },
        this);
  }

  _writeOptions._retryInterval = writeOptions._retryInterval;
  _writeOptions._defaultTags = writeOptions._defaultTags;
  _writeOptions._useServerTimestamp = writeOptions._useServerTimestamp;
  return true;
}

bool InfluxDBClient::setHTTPOptions(const HTTPOptions &httpOptions) {
  if (!_service && !init()) {
    return false;
  }
  _service->setHTTPOptions(httpOptions);
  return true;
}

BucketsClient *InfluxDBClient::getBucketsClient() {
  if (!_service && !init()) {
    return nullptr;
  }
  if (!_buckets) {
    _buckets.reset(new BucketsClient{&_connInfo, _service.get()});
  }
  return _buckets.get();
}

void InfluxDBClient::resetBuffer() {
  _writeBuffer->clear();
  INFLUXDB_CLIENT_DEBUG("[D] Reset buffer: buffer Size: %d, batch size: %d\n",
                        _writeOptions._bufferSize, _writeOptions._batchSize);
}

// Just change _bufferSize and reset the buffer, on next write a new buffer will
// be allocated based in the new buffer size
void InfluxDBClient::resizeBuffer(int size) {
  if (size > _writeOptions._bufferSize) {
    INFLUXDB_CLIENT_DEBUG("[D] Resizing buffer from %d to %d\n",
                          _writeOptions._bufferSize, size);
    _writeBuffer->_buffer.clear();
    _writeBuffer->_buffer.shrink_to_fit();
    _writeOptions._bufferSize = size;
  }
}

void InfluxDBClient::addZerosToTimestamp(Point &point, int zeroes) {
  point._data->timeStamp.append(zeroes, '0');
}

void InfluxDBClient::checkPrecisions(Point &point) {
  if (_writeOptions._writePrecision != WritePrecision::NoTime) {
    if (!point.hasTime()) {
      point.setTime(_writeOptions._writePrecision);
      // Check different write precisions
    } else if (point._data->tsWritePrecision != WritePrecision::NoTime &&
               point._data->tsWritePrecision != _writeOptions._writePrecision) {
      int diff = int(point._data->tsWritePrecision) -
                 int(_writeOptions._writePrecision);
      if (diff > 0) {  // point has higher precision, cut
        auto &timestamp = point._data->timeStamp;
        timestamp.erase(timestamp.length() - diff * 3);
      } else {  // point has lower precision, add zeroes
        addZerosToTimestamp(point, diff * -3);
      }
    }
    // check someone set WritePrecision on point and not on client. NS precision
    // is ok, cause it is default on server
  } else if (point.hasTime() &&
             point._data->tsWritePrecision != WritePrecision::NoTime &&
             point._data->tsWritePrecision != WritePrecision::NS) {
    int diff = int(WritePrecision::NS) - int(point._data->tsWritePrecision);
    addZerosToTimestamp(point, diff * 3);
  }
}

bool InfluxDBClient::writePoint(Point &point, bool chkBuffer) {
  if (point.hasFields()) {
    checkPrecisions(point);
    return writeRecord(pointToLineProtocol(point), chkBuffer);
  }
  return false;
}

InfluxDBClient::Batch::Batch(const uint16_t points) : _bufferSize(points) {}

InfluxDBClient::Batch::~Batch() { clear(); }

void InfluxDBClient::Batch::clear() {
  _buffer.clear();
  _numPoints = 0;
  _write = false;
  INFLUXDB_CLIENT_DEBUG("[D] Cleared buffer\n");
}

bool InfluxDBClient::Batch::append(const char *line) {
  INFLUXDB_CLIENT_DEBUG("[D] numPoints: %d _bufferSize %d\n", _numPoints,
                        _bufferSize);
  while (_numPoints > 0 && _buffer.length() + strlen(line) > _buffer.capacity()) {
    _buffer.erase(0, _buffer.find_first_of('\n') + 1);
    _numPoints--;
  }
  _buffer.append(line);
  _numPoints++;
  return isFull();
}

bool InfluxDBClient::writeRecord(const std::string &record, bool chkBuffer) {
  if (_streamWrite) {
    auto statusCode = postData(record.c_str());
    return statusCode >= 200 && statusCode < 300;
  }

  const auto bufferSize =
      _writeOptions._bufferSize * _writeOptions._batchSize * record.capacity();
  if (_writeBuffer->_buffer.capacity() < bufferSize) {
    _writeBuffer->_buffer.reserve(bufferSize);
  }

  if (_writeBuffer->append(record.c_str())) {
    _writeBuffer->_write = true;
    INFLUXDB_CLIENT_DEBUG("[D] Reached write batch size, marked for writing\n");
  }
  INFLUXDB_CLIENT_DEBUG("[D] done\n");
  return (chkBuffer) ? checkBuffer() : true;
}

bool InfluxDBClient::checkBuffer() {
  if (_writeBuffer->_write) {
    INFLUXDB_CLIENT_DEBUG("[D] Flushing buffer\n");
    return flushBufferInternal();
  }
  return false;
}

bool InfluxDBClient::flushBuffer() { return flushBufferInternal(); }

bool InfluxDBClient::flushBufferInternal() {
  if (!canSendRequest()) {
    INFLUXDB_CLIENT_DEBUG("[D] Still in retry interval, %d ticks remaining\n",
                          getRemainingRetryTime());
    return false;
  }

  auto success{false};
  if (validateConnection()) {
    // send all batches, It could happen there was long network outage and
    // buffer is full
    auto statusCode = postData(_writeBuffer->_buffer.c_str());
    // retry on unsuccessful connection or retryable status codes
    success = statusCode >= 200 && statusCode < 300;
  }
  INFLUXDB_CLIENT_DEBUG("[D] Last Write: %s\n",
                        (success) ? "Success" : "Failure");
  // advance even on message failure x e <300
  switch (success) {
    case true:
      _writeBuffer->clear();
      break;
    case false:
      _nextRetry =
          std::chrono::steady_clock::now() + _writeOptions._retryInterval;
      break;
  }
  return success;
}

const std::string &InfluxDBClient::pointToLineProtocol(Point &point) {
  return point.createLineProtocol(_writeOptions._defaultTags,
                                  _writeOptions._useServerTimestamp);
}

bool InfluxDBClient::validateConnection() {
  if (!_service && !init()) {
    return false;
  }

  INFLUXDB_CLIENT_DEBUG("[D] Validating connection to %s\n",
                        _validateUrl.c_str());

  if (!_service->doGET(_validateUrl.c_str(), 200, nullptr)) {
    INFLUXDB_CLIENT_DEBUG("[D] error %d: %s\n", _service->getLastStatusCode(),
                          _service->getLastErrorMessage().c_str());
    return false;
  }
  return true;
}

int InfluxDBClient::postData(const char *data) {
  if (!_service && !init()) {
    return 0;
  }
  if (data) {
    INFLUXDB_CLIENT_DEBUG("[D] Writing to %s\n", _writeUrl.c_str());
    // INFLUXDB_CLIENT_DEBUG("[D] Sending:\n%s\n", data);
    if (!_service->doPOST(_writeUrl.c_str(), data, PSTR("text/plain"), 204,
                          nullptr)) {
      INFLUXDB_CLIENT_DEBUG("[D] error %d: %s\n", _service->getLastStatusCode(),
                            _service->getLastErrorMessage().c_str());
    }
    return _service->getLastStatusCode();
  }
  return 0;
}

void InfluxDBClient::setStreamWrite(bool enable) {
  _streamWrite = enable;
  _writeBuffer->clear();
  switch (enable) {
    case true:
      _writeOptions._batchSize = 1;
      _writeOptions._bufferSize = 1;
      _writeBuffer->_buffer.reserve(0);
      _writeBuffer->_buffer.shrink_to_fit();
      break;
    case false:
      break;
  }
}

constexpr char QueryDialect[] PROGMEM =
    R"("dialect": {"annotations": ["datatype"],"dateTimeFormat": "RFC3339",)"
    R"("header": true,"delimiter": ",","commentPrefix": "#"})";

constexpr char Params[] PROGMEM = R"(,"params": {)";

FluxQueryResult InfluxDBClient::query(const std::string &fluxQuery) {
  return query(fluxQuery, QueryParams());
}

FluxQueryResult InfluxDBClient::query(const std::string &fluxQuery,
                                      QueryParams params) {
  if (_nextRetry != std::chrono::steady_clock::time_point::min() &&
      _nextRetry < std::chrono::steady_clock::now()) {
    auto left{std::to_string(
        (std::chrono::steady_clock::now() - _nextRetry).count())};
    INFLUXDB_CLIENT_DEBUG("[W] Cannot query yet, pause %ds, %ss yet\n",
                          _writeOptions._retryInterval.count(), left);
    // retry after period didn't run out yet
    std::string mess{TooEarlyMessage};
    mess.append(left);
    mess.push_back('s');
    return FluxQueryResult(mess);
  }
  if (!_service && !init()) {
    return FluxQueryResult(_connInfo.lastError);
  }
  INFLUXDB_CLIENT_DEBUG("[D] Query to %s\n", _queryUrl.c_str());
  INFLUXDB_CLIENT_DEBUG("[D] JSON query:\n%s\n", fluxQuery.c_str());

  auto queryEsc = escapeJSONString(fluxQuery);
  std::string body;
  body.reserve(150 + queryEsc.length() + params.size() * 30);
  body = R"({"type":"flux","query":")";
  body += queryEsc;
  body += R"(",)";
  body += QueryDialect;
  if (params.size()) {
    body += Params;
    body += params.jsonString(0);
    for (auto i = 1; i < params.size(); i++) {
      body += ",";
      body += params.jsonString(i);
    }
    body += '}';
  }
  body += '}';
  CsvReader *reader = nullptr;
  INFLUXDB_CLIENT_DEBUG("[D] Query: %s\n", body.c_str());
  if (_service->doPOST(
          _queryUrl.c_str(), body.c_str(), PSTR("application/json"), 200,
          [&](HTTPClient *httpClient) {
            bool chunked = false;
            if (httpClient->hasHeader(TransferEncoding)) {
              std::string header = httpClient->header(TransferEncoding).c_str();
              std::transform(header.begin(), header.end(), header.begin(),
                             [](unsigned char c) { return std::tolower(c); });
              chunked = header == "chunked";
            }
            INFLUXDB_CLIENT_DEBUG("[D] chunked: %s\n", bool2string(chunked));
            HttpStreamScanner *scanner =
                new HttpStreamScanner(httpClient, chunked);
            reader = new CsvReader(scanner);
            return false;
          })) {
    return FluxQueryResult(reader);
  } else {
    _nextRetry =
        std::chrono::steady_clock::now() + _writeOptions._retryInterval;
    return FluxQueryResult(_service->getLastErrorMessage());
  }
}

static std::string escapeJSONString(const std::string &value) {
  std::string ret;
  ret.reserve(value.length() + value.length() / 10);
  // most probably we will escape just double quotes
  for (char c : value) {
    switch (c) {
      case '"':
        ret.append("\\\"");
        break;
      case '\\':
        ret.append("\\\\");
        break;
      case '\b':
        ret.append("\\b");
        break;
      case '\f':
        ret.append("\\f");
        break;
      case '\n':
        ret.append("\\n");
        break;
      case '\r':
        ret.append("\\r");
        break;
      case '\t':
        ret.append("\\t");
        break;
      default:
        if (c <= '\x1f') {
          ret.append("\\u");
          char buf[3 + 8 * sizeof(unsigned int)];
          sprintf(buf, "\\u%04u", c);
          ret.append(buf);
        } else {
          ret.push_back(c);
        }
    }
  }
  return ret;
}
