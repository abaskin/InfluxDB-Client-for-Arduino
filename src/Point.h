/**
 *
 * Point.h: Point for write into InfluxDB server
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
#ifndef _POINT_H_
#define _POINT_H_

#include <Arduino.h>

#include <memory>
#include <string>

#include "WritePrecision.h"
#include "util/helpers.h"

/**
 * Class Point represents InfluxDB point in line protocol.
 * It defines data to be written to InfluxDB.
 */
class Point {
  friend class InfluxDBClient;

 public:
  Point(const std::string& measurement, const size_t lineSize = 128);
  Point(const Point& other);
  Point& operator=(const Point& other);
  virtual ~Point();
  // Adds string tag
  Point& addTag(const std::string& name, const std::string& value);
  // Add field with various types
  Point& addField(const std::string& name, float value, int decimalPlaces = 2);
  Point& addField(const std::string& name, double value, int decimalPlaces = 2);
  Point& addField(const std::string& name, char value);
  Point& addField(const std::string& name, unsigned char value);
  Point& addField(const std::string& name, int value);
  Point& addField(const std::string& name, unsigned int value);
  Point& addField(const std::string& name, long value);
  Point& addField(const std::string& name, unsigned long value);
  Point& addField(const std::string& name, bool value);
  Point& addField(const std::string& name, const std::string& value);
  Point& addField(const std::string& name, const __FlashStringHelper* pstr);
  Point& addField(const std::string& name, long long value);
  Point& addField(const std::string& name, unsigned long long value);
  // Set timestamp to `now()` and store it in specified precision, nanoseconds
  // by default. Date and time must be already set. See `configTime` in the
  // device API
  Point& setTime(WritePrecision writePrecision = WritePrecision::NS);
  // Set timestamp in offset since epoch (1.1.1970). Correct precision must be
  // set InfluxDBClient::setWriteOptions.
  Point& setTime(unsigned long long timestamp);
  // Set timestamp in offset since epoch (1.1.1970 00:00:00). Correct precision
  // must be set InfluxDBClient::setWriteOptions.
  Point& setTime(const std::string& timestamp);

  // Clear all fields. Useful for reusing point
  Point& clearFields();

  // Clear tags
  Point& clearTags();

  // True if a point contains at least one field. Points without a field cannot
  // be written to db
  bool hasFields() const { return !_data->fields.empty(); }

  // True if a point contains at least one tag
  bool hasTags() const { return !_data->tags.empty(); }

  // True if a point contains timestamp
  bool hasTime() const { return _data->hasTime(); }

  // Creates line protocol with optionally added tags
  const std::string& toLineProtocol(const std::string& includeTags = "");

  // returns current timestamp
  std::string& getTime() const { return _data->timeStamp; }

 protected:
  class Data {
   private:
    std::string line;

   public:
    Data(const std::string& _measurement, const size_t lineSize);
    ~Data();
    std::string measurement, tags, fields, timeStamp;
    WritePrecision tsWritePrecision;
    void addField(const std::string& name, const std::string& value,
                  const bool quote = false);
    void setTime(const std::string& timestamp);
    bool hasTime() const { return !timeStamp.empty(); }
    const std::string& createLineProtocol(const std::string& incTags,
                                          const bool excludeTimestamp = false);
  };
  std::shared_ptr<Data> _data;

 protected:
  // Creates line protocol string
  const std::string& createLineProtocol(const std::string& incTags,
                                        const bool excludeTimestamp = false);
};
#endif  //_POINT_H_
