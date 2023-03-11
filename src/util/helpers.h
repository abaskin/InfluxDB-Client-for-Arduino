/**
 *
 * helpers.h: InfluxDB Client util functions
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
#ifndef _INFLUXDB_CLIENT_HELPERS_H
#define _INFLUXDB_CLIENT_HELPERS_H

#include <Arduino.h>
#include <sys/time.h>

#include <memory>
#include <string>

// Synchronize time with NTP servers and waits for completition. Prints waiting
// progress and final synchronized time to the serial. Accurate time is
// necessary for certificate validion and writing points in batch For the
// fastest time sync find NTP servers in your area:
// https://www.pool.ntp.org/zone/
void timeSync(const char* tzInfo, const char* ntpServer1,
              const char* ntpServer2 = nullptr,
              const char* ntpServer3 = nullptr);

// Create timestamp in offset from epoch. secFracDigits specify resulution. 0 -
// seconds, 3 - milliseconds, etc. Maximum and default is 9 - nanoseconds.
unsigned long long getTimeStamp(struct timeval* tv,
                                const int secFracDigits = 3);

// Converts unsigned long long timestamp to std::string
std::string timeStampToString(const unsigned long long timestamp,
                              const int extraCharsSpace = 0);

// Escape invalid chars in measurement, tag key, tag value and field key
size_t escapeKey(std::string& str, const size_t start, const std::string& key,
                 const bool escapeEqual = true);

// Escape invalid chars in field value
size_t escapeValue(std::string& str, const size_t start,
                   const std::string& value);

// Encode URL string for invalid chars
std::string urlEncode(const std::string& src);
// Returns true of string contains valid InfluxDB ID type
bool isValidID(const std::string& idString);
// Returns "true" if val is true, otherwise "false"
const char* bool2string(const bool val);
// Returns number of digits
size_t getNumLength(const long long l);

bool endsWith(const std::string str, const std::string suffix);
bool startsWith(const std::string str, const std::string suffix);
void trim(std::string& str);

#endif  //_INFLUXDB_CLIENT_HELPERS_H