/**
 * 
 * Options.h: InfluxDB Client write options and HTTP options
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
#ifndef _OPTIONS_H_
#define _OPTIONS_H_

#include <chrono>
#include <string>

#include "WritePrecision.h"

class InfluxDBClient;
class HTTPService;
class Influxdb;
class Test;

/**
 * WriteOptions holds write related options
 */
class WriteOptions {
private:
    friend class InfluxDBClient;
    friend class Influxdb;
    friend class Test;
    // Points timestamp precision
    WritePrecision _writePrecision; 
    // Number of points that will be written to the databases at once. 
    // Default 1 (immediate write, no batching)
    uint16_t _batchSize;
    // Write buffer size - maximum number _batchSize buffers to keep.
    // When max size is reached, oldest records are overwritten.
    // Default 5
    uint16_t _bufferSize;
    // Maximum number of seconds points can be held in buffer before are written to the db. 
    // Buffer is flushed when it reaches batch size or when flush interval runs out.
    std::chrono::seconds _flushInterval;
    // Default retry interval in sec, if not sent to server. Default 5s. 
    // Setting to zero disables retrying.
    std::chrono::seconds _retryInterval;
    // Default tags. Default tags are added to every written point. 
    // There cannot be the same tags in the default tags and among the tags included with a point.
    std::string _defaultTags;
    //  Let server assign timestamp in given precision. Do not sent timestamp.
    bool _useServerTimestamp;
public:
 WriteOptions()
     : _writePrecision(WritePrecision::NoTime),
       _batchSize(1),
       _bufferSize(5),
       _flushInterval(std::chrono::seconds{60}),
       _retryInterval(std::chrono::seconds{5}),

       _useServerTimestamp(false) {}
 // Sets timestamp precision. If timestamp precision is set, but a point does
 // not have a timestamp, timestamp is automatically assigned from the device
 // clock. If useServerTimestamp is set to true, timestamp is not sent, only
 // precision is specified for the server.
 WriteOptions& writePrecision(WritePrecision precision) {
   _writePrecision = precision;
   return *this; }
    // Sets number of points that will be written to the databases at once. Points are added one by one and when number reaches batch size there are sent to server.
    WriteOptions& batchSize(uint16_t batchSize) { _batchSize = batchSize; return *this; }
    // Sets size of the write buffer to control maximum number of record to keep in case of write failures.
    // When max size is reached, oldest records are overwritten.
    WriteOptions& bufferSize(uint16_t bufferSize) { _bufferSize = bufferSize; return *this; }
    // Sets interval in seconds after which points will be written to the db. If
    WriteOptions& flushInterval(std::chrono::seconds flushIntervalSec) {
      _flushInterval = flushIntervalSec;
      return *this;
    }
    // Sets default retry interval in sec. This is used in case of network failure or if server is busy and doesn't specify retry interval.  
    // Setting to zero disables retrying.
    WriteOptions& retryInterval(std::chrono::seconds retryIntervalSec) {
      _retryInterval = retryIntervalSec;
      return *this;
    }
    // Adds new default tag. Default tags are added to every written point. 
    // There cannot be the same tag in the default tags and in the tags included with a point.
    WriteOptions& addDefaultTag(const std::string &name, const std::string &value);
    // Clears default tag list
    WriteOptions& clearDefaultTags() { _defaultTags.clear(); return *this; }
    // If timestamp precision is set and useServerTimestamp  is true, timestamp from point is not sent, or assigned.
    WriteOptions& useServerTimestamp(bool useServerTimestamp) { _useServerTimestamp = useServerTimestamp; return *this; }
};

/**
 * HTTPOptions holds HTTP related options
 */
class HTTPOptions {
private:    
    friend class InfluxDBClient;
    friend class HTTPService;
    friend class Influxdb;
    friend class Test;
    // true if HTTP connection should be kept open. Usable for frequent writes.
    // Default false.
    bool _connectionReuse;
    // Timeout [ms] for reading server response.
    // Default 5000ms  
    int _httpReadTimeout;
public:
    HTTPOptions():
        _connectionReuse(false),
        _httpReadTimeout(5000) {
        }
    // Set true if HTTP connection should be kept open. Usable for frequent writes.
    HTTPOptions& connectionReuse(bool connectionReuse) { _connectionReuse = connectionReuse; return *this; }
    // Sets timeout after which HTTP stops reading
    HTTPOptions& httpReadTimeout(int httpReadTimeoutMs) { _httpReadTimeout = httpReadTimeoutMs; return *this; }
};

#endif //_OPTIONS_H_
