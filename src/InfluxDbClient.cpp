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
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
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

#include "util/debug.h"

static const char TooEarlyMessage[] PROGMEM = "Cannot send request yet because of applied retry strategy. Remaining ";

static std::string escapeJSONString(const std::string &value);
static std::string precisionToString(WritePrecision precision, uint8_t version = 2) {
    switch(precision) {
        case WritePrecision::US:
            return version==1?"u":"us";
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


InfluxDBClient::InfluxDBClient() { 
   resetBuffer();
}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl, const std::string &db):InfluxDBClient() {
    setConnectionParamsV1(serverUrl, db);
}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl, const std::string &org, const std::string &bucket, const std::string &authToken):InfluxDBClient(serverUrl, org, bucket, authToken, nullptr) { 
}

InfluxDBClient::InfluxDBClient(const std::string &serverUrl, const std::string &org, const std::string &bucket, const std::string &authToken, const char *serverCert):InfluxDBClient() {
    setConnectionParams(serverUrl, org, bucket, authToken, serverCert);
}

void InfluxDBClient::setInsecure(bool value){
  _connInfo.insecure = value;
}

void InfluxDBClient::setConnectionParams(const std::string &serverUrl, const std::string &org, const std::string &bucket, const std::string &authToken, const char *certInfo) {
    clean();
    _connInfo.serverUrl = serverUrl;
    _connInfo.bucket = bucket;
    _connInfo.org = org;
    _connInfo.authToken = authToken;
    _connInfo.certInfo = certInfo;
    _connInfo.dbVersion = 2;
}

void InfluxDBClient::setConnectionParamsV1(const std::string &serverUrl, const std::string &db, const std::string &user, const std::string &password, const char *certInfo) {
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
    INFLUXDB_CLIENT_DEBUG("[D]  SDK version: " INFLUXDB_CLIENT_PLATFORM_VERSION "\n");
    INFLUXDB_CLIENT_DEBUG("[D]  Server url: %s\n", _connInfo.serverUrl.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  Org: %s\n", _connInfo.org.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  Bucket: %s\n", _connInfo.bucket.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  Token: %s\n", _connInfo.authToken.c_str());
    INFLUXDB_CLIENT_DEBUG("[D]  DB version: %d\n", _connInfo.dbVersion);
    if(_connInfo.serverUrl.length() == 0 || (_connInfo.dbVersion == 2 && (_connInfo.org.length() == 0 || _connInfo.bucket.length() == 0 || _connInfo.authToken.length() == 0))) {
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



InfluxDBClient::~InfluxDBClient() {
    _writeBuffer.clear();
    clean();
}

void InfluxDBClient::clean() {
    _buckets = nullptr;
    _lastFlushed = millis();
    _retryTime = 0;
}

bool InfluxDBClient::setUrls() {
    if(!_service && !init()) {
        return false;
    }
    INFLUXDB_CLIENT_DEBUG("[D] setUrls\n");
    if( _connInfo.dbVersion == 2) {
        _writeUrl = _service->getServerAPIURL();
        _writeUrl += "write?org=";
        _writeUrl +=  urlEncode(_connInfo.org.c_str());
        _writeUrl += "&bucket=";
        _writeUrl += urlEncode(_connInfo.bucket.c_str());
        INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
        _queryUrl = _service->getServerAPIURL();;
        _queryUrl += "query?org=";
        _queryUrl +=  urlEncode(_connInfo.org.c_str());
        INFLUXDB_CLIENT_DEBUG("[D]  queryUrl: %s\n", _queryUrl.c_str());
    } else {
        _writeUrl = _connInfo.serverUrl;
        _writeUrl += "/write?db=";
        _writeUrl += urlEncode(_connInfo.bucket.c_str());
        _queryUrl = _connInfo.serverUrl;
        _queryUrl += "/api/v2/query";
        if(_connInfo.user.length() > 0 && _connInfo.password.length() > 0) {
            std::string auth = "&u=";
            auth += urlEncode(_connInfo.user.c_str());
            auth += "&p=";
            auth += urlEncode(_connInfo.password.c_str());
            _writeUrl += auth;  
            _queryUrl += "?";
            _queryUrl += auth;
        }
        INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
        INFLUXDB_CLIENT_DEBUG("[D]  queryUrl: %s\n", _queryUrl.c_str());
    }
    if(_writeOptions._writePrecision != WritePrecision::NoTime) {
        _writeUrl += "&precision=";
        _writeUrl += precisionToString(_writeOptions._writePrecision, _connInfo.dbVersion);
        INFLUXDB_CLIENT_DEBUG("[D]  writeUrl: %s\n", _writeUrl.c_str());
    }
    return true;
}

bool InfluxDBClient::setWriteOptions(WritePrecision precision, uint16_t batchSize, uint16_t bufferSize, uint16_t flushInterval, bool preserveConnection) {
    if(!_service && !init()) {
        return false;
    }
    if(!setWriteOptions(WriteOptions().writePrecision(precision).batchSize(batchSize).bufferSize(bufferSize).flushInterval(flushInterval))) {
        return false;
    }
    if(!setHTTPOptions(_service->getHTTPOptions().connectionReuse(preserveConnection))) {
        return false;
    }
    return true;
}

bool InfluxDBClient::setWriteOptions(const WriteOptions & writeOptions) {
    if(_writeOptions._writePrecision != writeOptions._writePrecision) {
        _writeOptions._writePrecision = writeOptions._writePrecision;
        if(!setUrls()) {
            return false;
        }
    }
    bool writeBufferSizeChanges = false;
    if(writeOptions._batchSize > 0 && _writeOptions._batchSize != writeOptions._batchSize) {
        _writeOptions._batchSize = writeOptions._batchSize;
        writeBufferSizeChanges = true;
    }
    if(writeOptions._bufferSize > 0 && _writeOptions._bufferSize != writeOptions._bufferSize) {
        _writeOptions._bufferSize = writeOptions._bufferSize;
        if(_writeOptions._bufferSize <  2*_writeOptions._batchSize) {
            _writeOptions._bufferSize = 2*_writeOptions._batchSize;
            INFLUXDB_CLIENT_DEBUG("[D] Changing buffer size to %d\n", _writeOptions._bufferSize);
        }
        writeBufferSizeChanges = true;
    }
    if(writeBufferSizeChanges) {
        resetBuffer();
    }
    _writeOptions._flushInterval = writeOptions._flushInterval;
    _writeOptions._retryInterval = writeOptions._retryInterval;
    _writeOptions._maxRetryInterval = writeOptions._maxRetryInterval;
    _writeOptions._maxRetryAttempts = writeOptions._maxRetryAttempts;
    _writeOptions._defaultTags = writeOptions._defaultTags;
    _writeOptions._useServerTimestamp = writeOptions._useServerTimestamp;
    return true;
}

bool InfluxDBClient::setHTTPOptions(const HTTPOptions & httpOptions) {
    if(!_service && !init()) {
        return false;
    }
    _service->setHTTPOptions(httpOptions);
    return true;
}

BucketsClient InfluxDBClient::getBucketsClient() {
    if(!_service && !init()) {
        return BucketsClient();
    }
    if(!_buckets) {
        _buckets = BucketsClient(&_connInfo, _service.get());
    }
    return _buckets;
}

void InfluxDBClient::resetBuffer() {
    _writeBuffer.clear();
    INFLUXDB_CLIENT_DEBUG("[D] Reset buffer: buffer Size: %d, batch size: %d\n", _writeOptions._bufferSize, _writeOptions._batchSize);
    uint16_t a = _writeOptions._bufferSize/_writeOptions._batchSize;
    //limit to max(byte)
    _writeBufferSize = a >= (1 << 8) ? (1 << 8) - 1:a;
    if(_writeBufferSize < 2) {
        _writeBufferSize = 2;
    }
    INFLUXDB_CLIENT_DEBUG("[D] Reset buffer: writeBuffSize: %d\n", _writeBufferSize);
    _bufferPointer = 0;
    _batchPointer = 0;
    _bufferCeiling = 0;
}

void InfluxDBClient::reserveBuffer(int size) {
    if(size > _writeBufferSize) {
        INFLUXDB_CLIENT_DEBUG("[D] Resizing buffer from %d to %d\n",_writeBufferSize, size);
        _writeBuffer.resize(size);
        _writeBufferSize = size;
    }
}

void InfluxDBClient::addZerosToTimestamp(Point &point, int zeroes) {
    point._data->timestamp.append(zeroes, '0');
}

void InfluxDBClient::checkPrecisions(Point & point) {
    if(_writeOptions._writePrecision != WritePrecision::NoTime) {
        if(!point.hasTime()) {
            point.setTime(_writeOptions._writePrecision);
        // Check different write precisions
        } else if(point._data->tsWritePrecision != WritePrecision::NoTime && point._data->tsWritePrecision != _writeOptions._writePrecision) {
            int diff = int(point._data->tsWritePrecision) - int(_writeOptions._writePrecision);
            if(diff > 0) { //point has higher precision, cut 
                point._data->timestamp.erase(point._data->timestamp.length()-diff*3);
            } else { //point has lower precision, add zeroes
                addZerosToTimestamp(point, diff*-3);
            }
        }
    // check someone set WritePrecision on point and not on client. NS precision is ok, cause it is default on server
    } else if(point.hasTime() && point._data->tsWritePrecision != WritePrecision::NoTime && point._data->tsWritePrecision != WritePrecision::NS) {
        int diff = int(WritePrecision::NS) - int(point._data->tsWritePrecision);
        addZerosToTimestamp(point, diff*3);
    } 
}

bool InfluxDBClient::writePoint(Point & point) {
    if (point.hasFields()) {
        checkPrecisions(point);
        auto line = pointToLineProtocol(point);
        return writeRecord(line);
    }
    return false;
}



InfluxDBClient::Batch::Batch(uint16_t size):_size(size) {  
    buffer.reserve(size); 
}


InfluxDBClient::Batch::~Batch() { 
    clear();
}

void InfluxDBClient::Batch::clear() {
    buffer.clear();
}

bool InfluxDBClient::Batch::append(const char *line) {
    if(buffer.size() == _size) {
        clear();
    } 
    buffer.emplace_back(line);
    return isFull();
}

std::string InfluxDBClient::Batch::createData() {
    size_t length { 0 };
    std::for_each(buffer.begin(), buffer.end(),[&length](const std::string& b){
        length += b.length();
        yield();
    });
    std::string buff;
    buff.reserve(length + buffer.size());
    std::for_each(buffer.begin(), buffer.end(),
                  [&buff](const std::string& b) { buff.append(b + "\n"); });
    return buff;
}

bool InfluxDBClient::writeRecord(const std::string &record) {
    return writeRecord(record.c_str());
}

bool InfluxDBClient::writeRecord(const char *record) {
    if (!_writeBuffer[_bufferPointer]) {
        _writeBuffer.emplace_back(new Batch{_writeOptions._batchSize});
    }
    if(isBufferFull() && _batchPointer <= _bufferPointer) {
        // When we are overwriting buffer and nothing is written, batchPointer must point to the oldest point
        _batchPointer = _bufferPointer + 1;
        if (_batchPointer == _writeBufferSize) {
            _batchPointer = 0;
        }
    }
    if(_writeBuffer[_bufferPointer]->append(record)) { //we reached batch size
        _bufferPointer++;
        if(_bufferPointer == _writeBufferSize) { // writeBuffer is full
            _bufferPointer = 0;
            INFLUXDB_CLIENT_DEBUG("[W] Reached write buffer size, old points will be overwritten\n");
        } 

        if(_bufferCeiling < _writeBufferSize) {
            _bufferCeiling++;
        }
    } 
    INFLUXDB_CLIENT_DEBUG("[D] writeRecord: bufferPointer: %d, batchPointer: %d, _bufferCeiling: %d\n", _bufferPointer, _batchPointer, _bufferCeiling);    
    return checkBuffer();
}

bool InfluxDBClient::checkBuffer() {
    // in case we (over)reach batchSize with non full buffer
    bool bufferReachedBatchsize = _writeBuffer[_batchPointer] && _writeBuffer[_batchPointer]->isFull();
    // or flush interval timed out
    bool flushTimeout = _writeOptions._flushInterval > 0 && ((millis() - _lastFlushed)/1000) >= _writeOptions._flushInterval; 

    INFLUXDB_CLIENT_DEBUG("[D] Flushing buffer: is oversized %s, is timeout %s, is buffer full %s\n", 
        bool2string(bufferReachedBatchsize),bool2string(flushTimeout), bool2string(isBufferFull()));
    
    if(bufferReachedBatchsize || flushTimeout || isBufferFull() ) {
        
       return flushBufferInternal(!flushTimeout);
    } 
    return true;
}

bool InfluxDBClient::flushBuffer() {
    return flushBufferInternal(false);
}

uint32_t InfluxDBClient::getRemainingRetryTime() {
    uint32_t rem = 0;
    if(_retryTime > 0) {
        int32_t diff = _retryTime - (millis()-_service->getLastRequestTime())/1000;
        rem  =  diff<0?0:(uint32_t)diff;
    }
    return rem;
}

bool InfluxDBClient::flushBufferInternal(bool flashOnlyFull) {
    uint32_t rwt = getRemainingRetryTime();
    if(rwt > 0) {
        INFLUXDB_CLIENT_DEBUG("[W] Cannot write yet, pause %ds, %ds yet\n", _retryTime, rwt);
        // retry after period didn't run out yet
        _connInfo.lastError = TooEarlyMessage;
        _connInfo.lastError += std::to_string(rwt);
        _connInfo.lastError += "s";
        return false;
    }
    bool success = true;
    // send all batches, It could happen there was long network outage and buffer is full
    while(_batchPointer < _writeBuffer.size() && (!flashOnlyFull ||  _writeBuffer[_batchPointer]->isFull())) {
        if(!_writeBuffer[_batchPointer]->isFull() && _writeBuffer[_batchPointer]->retryCount == 0 ) { //do not increase pointer in case of retrying
            // points will be written so increase _bufferPointer as it happen when buffer is flushed when is full
            if(++_bufferPointer == _writeBufferSize) {
                _bufferPointer = 0;
            }
        }

        INFLUXDB_CLIENT_DEBUG("[D] Writing batch, batchpointer: %d, size %d\n", _batchPointer, _writeBuffer[_batchPointer]->pointer);
        if(!_writeBuffer[_batchPointer]->isEmpty()) {
            int statusCode = 0;
            if(_streamWrite) {
                statusCode = postData(_writeBuffer[_batchPointer].get());
            } else {
                statusCode = postData(_writeBuffer[_batchPointer]->createData().c_str());
            }
            // retry on unsuccessfull connection or retryable status codes
            bool retry = (statusCode < 0 || statusCode >= 429) && _writeOptions._maxRetryAttempts > 0;
            success = statusCode >= 200 && statusCode < 300;
            // advance even on message failure x e <300;429)
            if(success || !retry) {
                _lastFlushed = millis();
                dropCurrentBatch();
            } else if(retry) {
                _writeBuffer[_batchPointer]->retryCount++;
                if(statusCode > 0) { //apply retry strategy only in case of HTTP errors
                    if(_writeBuffer[_batchPointer]->retryCount > _writeOptions._maxRetryAttempts) {
                        INFLUXDB_CLIENT_DEBUG("[D] Reached max retry count, dropping batch\n");
                        dropCurrentBatch();
                    }
                    if(!_retryTime) {
                        _retryTime = _writeOptions._retryInterval;
                        if(_batchPointer < _writeBuffer.size()) {
                            for(int i = 1; i < _writeBuffer[_batchPointer]->retryCount; i++) {
                                _retryTime *= _writeOptions._retryInterval;
                            }
                            if(_retryTime > _writeOptions._maxRetryInterval) {
                                _retryTime = _writeOptions._maxRetryInterval;
                            }
                        }
                    }
                } 
                INFLUXDB_CLIENT_DEBUG("[D] Leaving data in buffer for retry, retryInterval: %d\n",_retryTime);
                // in case of retryable failure break loop
                break;
            }
        }
       yield();
    }
    //Have we emptied the buffer?
    INFLUXDB_CLIENT_DEBUG("[D] Success: %d, _bufferPointer: %d, _batchPointer: %d, _writeBuffer[_bufferPointer]_%p\n",success,_bufferPointer,_batchPointer, _writeBuffer[_bufferPointer]);
    if(_batchPointer == _bufferPointer && !_writeBuffer[_bufferPointer]) {
        _bufferPointer = 0;
        _batchPointer = 0;
        _bufferCeiling = 0;
        INFLUXDB_CLIENT_DEBUG("[D] Buffer empty\n");
    }
    return success;
}

void  InfluxDBClient::dropCurrentBatch() {
    _writeBuffer.erase(_writeBuffer.begin() + _batchPointer);
    _batchPointer++;
    //did we got over top?
    if(_batchPointer == _writeBufferSize) {
        // restart _batchPointer in ring buffer from start
        _batchPointer = 0;
        // we reached buffer size, that means buffer was full and now lower ceiling 
        _bufferCeiling = _bufferPointer;
    }
    INFLUXDB_CLIENT_DEBUG("[D] Dropped batch, batchpointer: %d\n", _batchPointer);
}

std::string InfluxDBClient::pointToLineProtocol(const Point& point) {
    return point.createLineProtocol(_writeOptions._defaultTags, _writeOptions._useServerTimestamp);
}

bool InfluxDBClient::validateConnection() {
    if(!_service && !init()) {
        return false;
    }
    // on version 1.x /ping will by default return status code 204, without verbose
    auto url = _connInfo.serverUrl + (_connInfo.dbVersion==2?"/health":"/ping?verbose=true");
    if(_connInfo.dbVersion==1 && _connInfo.user.length() > 0 && _connInfo.password.length() > 0) {
        url += "&u=";
        url += urlEncode(_connInfo.user.c_str());
        url += "&p=";
        url += urlEncode(_connInfo.password.c_str());
    }
    INFLUXDB_CLIENT_DEBUG("[D] Validating connection to %s\n", url.c_str());

    bool ret = _service->doGET(url.c_str(), 200, nullptr);
    if(!ret) {
        INFLUXDB_CLIENT_DEBUG("[D] error %d: %s\n", _service->getLastStatusCode(), _service->getLastErrorMessage().c_str());
    }
    return ret;
}

int InfluxDBClient::postData(const char *data) {
    if(!_service && !init()) {
        return 0;
    }
    if(data) {
        INFLUXDB_CLIENT_DEBUG("[D] Writing to %s\n", _writeUrl.c_str());
        INFLUXDB_CLIENT_DEBUG("[D] Sending:\n%s\n", data);       
        if(!_service->doPOST(_writeUrl.c_str(), data, PSTR("text/plain"), 204, nullptr)) {
            INFLUXDB_CLIENT_DEBUG("[D] error %d: %s\n", _service->getLastStatusCode(), _service->getLastErrorMessage().c_str());
        }
        _retryTime = _service->getLastRetryAfter();
        return _service->getLastStatusCode();
    } 
    return 0;
}

int InfluxDBClient::postData(Batch *batch) {
    if(!_service && !init()) {
        return 0;
    }

    std::unique_ptr<BatchStreamer> bs { new BatchStreamer(batch) }; 
    INFLUXDB_CLIENT_DEBUG("[D] Writing to %s\n", _writeUrl.c_str());
    INFLUXDB_CLIENT_DEBUG("[D] Sending %d:\n", bs->available());       
    
    if(!_service->doPOST(_writeUrl.c_str(), bs.get(), PSTR("text/plain"), 204, nullptr)) {
        INFLUXDB_CLIENT_DEBUG("[D] error %d: %s\n", _service->getLastStatusCode(), _service->getLastErrorMessage().c_str());
    }
    _retryTime = _service->getLastRetryAfter();
    return _service->getLastStatusCode();
}

void InfluxDBClient::setStreamWrite(bool enable) {
    _streamWrite = enable;
}


static const char QueryDialect[] PROGMEM = "\
\"dialect\": {\
\"annotations\": [\
\"datatype\"\
],\
\"dateTimeFormat\": \"RFC3339\",\
\"header\": true,\
\"delimiter\": \",\",\
\"commentPrefix\": \"#\"\
}";

static const char Params[] PROGMEM = ",\
\"params\": {";

FluxQueryResult InfluxDBClient::query(const std::string &fluxQuery) {
    return query(fluxQuery, QueryParams());
}

FluxQueryResult InfluxDBClient::query(const std::string &fluxQuery, QueryParams params) {
    uint32_t rwt = getRemainingRetryTime();
    if(rwt > 0) {
        INFLUXDB_CLIENT_DEBUG("[W] Cannot query yet, pause %ds, %ds yet\n", _retryTime, rwt);
        // retry after period didn't run out yet
        std::string mess = TooEarlyMessage;
        mess += std::to_string(rwt);
        mess += "s";
        return FluxQueryResult(mess);
    }
    if(!_service && !init()) {
        return FluxQueryResult(_connInfo.lastError);
    }
    INFLUXDB_CLIENT_DEBUG("[D] Query to %s\n", _queryUrl.c_str());
    INFLUXDB_CLIENT_DEBUG("[D] JSON query:\n%s\n", fluxQuery.c_str());

    auto queryEsc = escapeJSONString(fluxQuery);
    std::string body;
    body.reserve(150 + queryEsc.length() + params.size()*30);
    body = "{\"type\":\"flux\",\"query\":\"";
    body +=  queryEsc;
    body += "\",";
    body += QueryDialect;
    if(params.size()) {
        body += Params;
        body += params.jsonString(0);
        for(auto i = 1; i < params.size(); i++) {
            body +=",";
            body += params.jsonString(i);
        }
        body += '}';
    }
    body += '}';
    CsvReader *reader = nullptr;
    _retryTime = 0;
    INFLUXDB_CLIENT_DEBUG("[D] Query: %s\n", body.c_str());
    if(_service->doPOST(_queryUrl.c_str(), body.c_str(), PSTR("application/json"), 200, [&](HTTPClient *httpClient){
        bool chunked = false;
        if(httpClient->hasHeader(TransferEncoding)) {
            std::string header = httpClient->header(TransferEncoding).c_str();
            std::transform(header.begin(), header.end(), header.begin(),
                [](unsigned char c){ return std::tolower(c); });
            chunked = header == "chunked";
        }
        INFLUXDB_CLIENT_DEBUG("[D] chunked: %s\n", bool2string(chunked));
        HttpStreamScanner *scanner = new HttpStreamScanner(httpClient, chunked);
        reader = new CsvReader(scanner);
        return false;
    })) {
        return FluxQueryResult(reader);
    } else {
        _retryTime = _service->getLastRetryAfter();
        return FluxQueryResult(_service->getLastErrorMessage());
    }
}


static std::string escapeJSONString(const std::string &value) {
    std::string ret;
    int d = 0;
    int from = 0;
    size_t i;
    while((i = value.find('"',from)) != std::string::npos) {
        d++;
        if(i == value.length()-1) {
            break;
        }
        from = i+1;
    }
    ret.reserve(value.length()+d); //most probably we will escape just double quotes
    for (char c: value)
    {
        switch (c)
        {
            case '"': ret += "\\\""; break;
            case '\\': ret += "\\\\"; break;
            case '\b': ret += "\\b"; break;
            case '\f': ret += "\\f"; break;
            case '\n': ret += "\\n"; break;
            case '\r': ret += "\\r"; break;
            case '\t': ret += "\\t"; break;
            default:
                if (c <= '\x1f') {
                    ret += "\\u";
                    char buf[3 + 8 * sizeof(unsigned int)];
                    sprintf(buf,  "\\u%04u", c);
                    ret += buf;
                } else {
                    ret += c;
                }
        }
    }
    return ret;
}

InfluxDBClient::BatchStreamer::BatchStreamer(InfluxDBClient::Batch *batch) {
    _batch = batch;
    _read = 0;
    _length = 0;
    _pointer = 0;
    _linePointer = 0;
    std::for_each(_batch->buffer.begin(), _batch->buffer.end(),
        [this](const std::string& b) {
        _length += b.length();
        yield();
    });
}

int InfluxDBClient::BatchStreamer::available() {
    return _length-_read;
}

int InfluxDBClient::BatchStreamer::availableForWrite() {
    return 0;
}

void InfluxDBClient::BatchStreamer::reset() {
    _read = 0;
    _pointer = 0;
    _linePointer = 0;
}

int InfluxDBClient::BatchStreamer::read(uint8_t* buffer, size_t len) {
    INFLUXDB_CLIENT_DEBUG("BatchStream::read\n");
    return readBytes((char *)buffer, len);
}

size_t InfluxDBClient::BatchStreamer::readBytes(char* buffer, size_t len) {
#if defined(ESP8266)
    INFLUXDB_CLIENT_DEBUG("BatchStream::readBytes %d, free_heap %d, max_alloc_heap %d, heap_fragmentation  %d\n", len, ESP.getFreeHeap(), ESP.getMaxFreeBlockSize(), ESP.getHeapFragmentation());
#elif defined(ESP32)
    INFLUXDB_CLIENT_DEBUG("BatchStream::readBytes %d, free_heap %d, max_alloc_heap %d\n", len, ESP.getFreeHeap(), ESP.getMaxAllocHeap());
#endif
    unsigned int r=0;
    for(unsigned int i=0;i<len;i++) {
        if(available()) {
            buffer[i] = read();
            r++;
        } else {
            break;
        }
    }
    return r;
}

int InfluxDBClient::BatchStreamer::read()  {
    int r = peek();
    if(r > 0) {
        ++_read;
        ++_linePointer;
        if(!_batch->buffer[_pointer][_linePointer-1]) {
            ++_pointer;
            _linePointer = 0;
        }
    }
    return r;
}

int InfluxDBClient::BatchStreamer::peek() {
    if(_pointer == _batch->pointer) {
        //This should not happen
        return -1;
    }
    
    int r;
    if(!_batch->buffer[_pointer][_linePointer]) {
        r = '\n';
    } else {
        r = _batch->buffer[_pointer][_linePointer];
    }
    return r;
}

size_t InfluxDBClient::BatchStreamer::write(uint8_t)  {
    return 0;
}
