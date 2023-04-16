/**
 *
 * InfluxDBClient.h: InfluxDB Client for Arduino
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
#ifndef _INFLUXDB_CLIENT_H_
#define _INFLUXDB_CLIENT_H_

#include <Arduino.h>
#include <Ticker.h>

#include "BucketsClient.h"
#include "HTTPService.h"
#include "Options.h"
#include "Point.h"
#include "Version.h"
#include "WritePrecision.h"
#include "query/FluxParser.h"
#include "query/Params.h"
#include "util/debug.h"
#include "util/helpers.h"

#ifdef USING_AXTLS
#error AxTLS does not work
#endif

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

class Test;

/**
 * InfluxDBClient handles connection and basic operations for an InfluxDB
 * server. It provides write API with ability to write data in batches and
 * retrying failed writes. Automaticaly retries failed writes during next write,
 * if server is overloaded.
 */
class InfluxDBClient {
  friend class Test;

 public:
  // Creates InfluxDBClient unconfigured instance.
  // Call to setConnectionParams is required to set up client
  InfluxDBClient();
  // Creates InfluxDBClient instance for unsecured connection to InfluxDB 1
  // serverUrl - url of the InfluxDB 1 server (e.g. http://localhost:8086)
  // db - database name where to store or read data
  InfluxDBClient(const std::string &serverUrl, const std::string &db);
  // Creates InfluxDBClient instance for unsecured connection
  // serverUrl - url of the InfluxDB 2 server (e.g. http://localhost:8086)
  // org - name of the organization, which bucket belongs to
  // bucket - name of the bucket to write data into
  // authToken - InfluxDB 2 authorization token
  InfluxDBClient(const std::string &serverUrl, const std::string &org,
                 const std::string &bucket, const std::string &authToken);
  // Creates InfluxDBClient instance for secured connection
  // serverUrl - url of the InfluxDB 2 server (e.g. https://localhost:8086)
  // org - name of the organization, which bucket belongs to
  // bucket - name of the bucket to write data into
  // authToken - InfluxDB 2 authorization token
  // certInfo - InfluxDB 2 server trusted certificate (or CA certificate) or
  // certificate SHA1 fingerprint. Should be stored in PROGMEM.
  InfluxDBClient(const std::string &serverUrl, const std::string &org,
                 const std::string &bucket, const std::string &authToken,
                 const char *certInfo);
  // Clears instance.
  ~InfluxDBClient();
  // Allows insecure connection by skipping server certificate validation.
  // setInsecure must be called before calling any method initiating a
  // connection to server.
  void setInsecure(bool value = true);
  // Sets custom write options.
  // Must be called before calling any method initiating a connection to server.
  // precision - timestamp precision of written data
  // batchSize - number of points that will be written to the databases at once.
  // Default 1 - writes immediately bufferSize - maximum size of Points buffer.
  // Buffer contains new data that will be written to the database
  //             and also data that failed to be written due to network failure
  //             or server overloading
  // flushInterval - maximum number of seconds data will be held in buffer
  // before are written to the db.
  //                 Data are written either when number of points in buffer
  //                 reaches batchSize or time of
  // preserveConnection - true if HTTP connection should be kept open. Usable
  // for frequent writes. Returns true if setting was successful. Otherwise
  // check getLastErrorMessage() for an error.
  bool setWriteOptions(
      WritePrecision precision, uint16_t batchSize = 1, uint16_t bufferSize = 5,
      std::chrono::seconds flushInterval = std::chrono::seconds{60},
      bool preserveConnection = true)
      __attribute__((
          deprecated("Use setWriteOptions(const WriteOptions &writeOptions)")));
  // Sets custom write options. See WriteOptions doc for more info.
  // Must be called before calling any method initiating a connection to server.
  // Returns true if setting was successful. Otherwise check
  // getLastErrorMessage() for an error. Example:
  //    client.setWriteOptions(WriteOptions().batchSize(10).bufferSize(50)).
  bool setWriteOptions(const WriteOptions &writeOptions);
  // Sets custom HTTP options. See HTTPOptions doc for more info.
  // Must be called before calling any method initiating a connection to server.
  // Returns true if setting was successful. Otherwise check
  // getLastErrorMessage() for an error. Example:
  //    client.setHTTPOptions(HTTPOptions().httpReadTimeout(20000)).
  bool setHTTPOptions(const HTTPOptions &httpOptions);
  // Sets connection parameters for InfluxDB 2
  // Must be called before calling any method initiating a connection to server.
  // serverUrl - url of the InfluxDB 2 server (e.g. https//localhost:8086)
  // org - name of the organization, which bucket belongs to
  // bucket - name of the bucket to write data into
  // authToken - InfluxDB 2 authorization token
  // serverCert - Optional. InfluxDB 2 server trusted certificate (or CA
  // certificate) or certificate SHA1 fingerprint.  Should be stored in PROGMEM.
  // Only in case of https connection.
  void setConnectionParams(const std::string &serverUrl, const std::string &org,
                           const std::string &bucket,
                           const std::string &authToken,
                           const char *certInfo = nullptr);
  // Sets parameters for connection to InfluxDB 1
  // Must be called before calling any method initiating a connection to server.
  // serverUrl - url of the InfluxDB server (e.g. http://localhost:8086)
  // db - database name where to store or read data
  // user - Optional. User name, in case of server requires authetication
  // password - Optional. User password, in case of server requires
  // authetication certInfo - Optional. InfluxDB server trusted certificate (or
  // CA certificate) or certificate SHA1 fingerprint.  Should be stored in
  // PROGMEM. Only in case of https connection.
  void setConnectionParamsV1(const std::string &serverUrl,
                             const std::string &db,
                             const std::string &user = "",
                             const std::string &password = "",
                             const char *certInfo = nullptr);
  // Creates line protocol string from point data and optional default tags set
  // in WriteOptions.
  const std::string &pointToLineProtocol(Point &point);
  // Validates connection parameters by connecting to server
  // Returns true if successful, false in case of any error
  bool validateConnection();
  // Writes record in InfluxDB line protocol format to write buffer.
  // Returns true if successful, false in case of any error
  bool writeRecord(const std::string &record, bool chkBuffer = true);
  // Writes record represented by Point to buffer
  // Returns true if successful, false in case of any error
  bool writePoint(Point &point, bool chkBuffer = true);
  // Sends Flux query and returns FluxQueryResult object for subsequently
  // reading flux query response. Use FluxQueryResult::next() method to iterate
  // over lines of the query result. Always call of FluxQueryResult::close()
  // when reading is finished. Check FluxQueryResult doc for more info.
  FluxQueryResult query(const std::string &fluxQuery);
  // Sends Flux query with params and returns FluxQueryResult object for
  // subsequently reading flux query response. Use FluxQueryResult::next()
  // method to iterate over lines of the query result. Always call of
  // FluxQueryResult::close() when reading is finished. Check FluxQueryResult
  // doc for more info.
  FluxQueryResult query(const std::string &fluxQuery, QueryParams params);
  // Forces writing of all points in buffer, even the batch is not full.
  // Returns true if successful, false in case of any error
  bool flushBuffer();
  // Returns true if points buffer is full. Useful when server is overloaded and
  // we may want increase period of write points or decrease number of points
  bool isBufferFull() const { return _writeBuffer->isFull(); };
  // Returns true if buffer is empty. Useful when going to sleep and check if
  // there is sth in write buffer (it can happens when batch size if bigger than
  // 1). Call flushBuffer() then.
  bool isBufferEmpty() const { return _writeBuffer->isEmpty(); };
  // Checks points buffer status and flushes if number of points reached batch
  // size or flush interval runs out. Returns true if anything is flushed
  // successfully, false if not
  bool checkBuffer();
  // Wipes out buffered points
  void resetBuffer();
  // Returns HTTP status of last request to server. Useful for advanced handling
  // of failures.
  int getLastStatusCode() const {
    return _service ? _service->getLastStatusCode() : 0;
  }
  // Returns last response when operation failed
  std::string getLastErrorMessage() const { return _connInfo.lastError; }
  // Returns server url
  std::string getServerUrl() const { return _connInfo.serverUrl; }
  // Check if it is possible to send write/query request to server.
  // Returns true if write or query can be send, or false, if server is
  // overloaded and retry strategy is applied. Use getRemainingRetryTime() to
  // get wait time in such case.
  bool canSendRequest() {
    return _nextRetry <= std::chrono::steady_clock::now();
  }
  // Returns remaining wait time in seconds when retry strategy is applied.
  uint32_t getRemainingRetryTime() {
    if (canSendRequest()) return 0;
    return (std::chrono::steady_clock::time_point::min() - _nextRetry).count();
  };
  // Returns sub-client for managing buckets
  BucketsClient *getBucketsClient();
  // Enables/disables streaming write. This allows sending large batches without
  // allocating buffer. It is about 50% slower than writing by allocated buffer
  // (default);
  void setStreamWrite(bool enable = true);
  // Returns true if HTTP connection is kept open (connection reuse must be set
  // to true)
  bool isConnected() const { return _service && _service->isConnected(); }

 protected:
  // Checks params and sets up security, if needed.
  // Returns true in case of success, otherwise false
  bool init();
  // Cleans instances
  void clean();

 protected:
  class Batch {
    friend class Test;

   private:
    uint32_t _bufferSize{0};
    uint32_t _numPoints{0};

   public:
    std::atomic<bool> _write{false};
    std::string _buffer;
    Batch(const uint16_t points = 1);
    ~Batch();
    bool append(const char *line);
    void clear();
    bool isFull() const { return _numPoints >= _bufferSize; }
    bool isEmpty() const { return _buffer.empty(); }
    void setBufferSize(const uint32_t points = 1) { _bufferSize = points; };
    uint32_t getBufferSize() { return _bufferSize; };
    uint32_t getNumPoints() { return _numPoints; };
  };

  ConnectionInfo _connInfo;
  // Cached full write url
  std::string _writeUrl;
  // Cached full query url
  std::string _queryUrl;
  // Cached validate url
  std::string _validateUrl;
  // Points buffer
  std::unique_ptr<Batch> _writeBuffer;
  // Write options
  WriteOptions _writeOptions;
  // Ticker to mark batch for write
  Ticker _flushTicker;
  // HTTP operations object
  std::unique_ptr<HTTPService> _service;
  // Bucket sub-client
  std::unique_ptr<BucketsClient> _buckets;
  // Write using buffer or stream
  bool _streamWrite = false;
  // next retry time
  std::chrono::steady_clock::time_point _nextRetry{
      std::chrono::steady_clock::time_point::min()};

 protected:
  // Sends POST request with data in body
  int postData(const char *data);
  // Sets cached InfluxDB server API URLs
  bool setUrls();
  // Resize the buffer to the required size
  void resizeBuffer(int size);
  // Writes all points in buffer, with respect to the batch size, and in case of
  // success clears the buffer.
  // Returns true if successful, false in case of any error
  bool flushBufferInternal();
  // Checks precision of point and modifies if needed
  void checkPrecisions(Point &point);
  // helper which adds zeroes to timestamp of point to increase precision
  static void addZerosToTimestamp(Point &point, int zeroes);
};

#endif  //_INFLUXDB_CLIENT_H_