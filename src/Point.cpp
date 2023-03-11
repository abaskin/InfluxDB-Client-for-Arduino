/**
 *
 * Point.cpp: Point for write into InfluxDB server
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

#include "Point.h"

#include "util/helpers.h"

Point::Point(const std::string& measurement, const size_t lineSize) {
  std::string m;
  escapeKey(m, 0, measurement, false);
  _data = std::make_shared<Data>(m, lineSize);
}

Point::~Point() {}

Point::Data::Data(const std::string& _measurement, const size_t _lineSize)
    : measurement(_measurement), tsWritePrecision(WritePrecision::NoTime) {
  line.reserve(_lineSize);
}

Point::Data::~Data() {}

Point::Point(const Point& other) { *this = other; }

Point& Point::operator=(const Point& other) {
  if (this != &other) {
    this->_data = other._data;
  }
  return *this;
}

Point& Point::addTag(const std::string& name, const std::string& value) {
  escapeKey(_data->tags, _data->tags.length(), name);
  escapeKey(_data->tags, _data->tags.length(), value);
  _data->tags.push_back(',');  // add a comma and pop it off later if required

  return *this;
}

Point& Point::addField(const std::string& name, long long value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, unsigned long long value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name,
                       const __FlashStringHelper* pstr) {
  std::unique_ptr<char[]> buff{new char[strlen_P((PGM_P)pstr) + 1]};
  strcpy_P(buff.get(), (PGM_P)pstr);
  _data->addField(name, buff.get());
  return *this;
}

Point& Point::addField(const std::string& name, float value,
                       int decimalPlaces) {
  if (!isnan(value))
    _data->addField(name, String(value, decimalPlaces).c_str());
  return *this;
}

Point& Point::addField(const std::string& name, double value,
                       int decimalPlaces) {
  if (!isnan(value))
    _data->addField(name, String(value, decimalPlaces).c_str());
  return *this;
}

Point& Point::addField(const std::string& name, char value) {
  _data->addField(name, std::string{value});
  return *this;
}

Point& Point::addField(const std::string& name, unsigned char value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, int value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, unsigned int value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, long value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, unsigned long value) {
  _data->addField(name, std::to_string(value) + "i");
  return *this;
}

Point& Point::addField(const std::string& name, bool value) {
  _data->addField(name, bool2string(value));
  return *this;
}

Point& Point::addField(const std::string& name, const std::string& value) {
  _data->addField(name, value, true);
  return *this;
}

void Point::Data::addField(const std::string& name, const std::string& value,
                           const bool quote) {
  escapeKey(fields, fields.length(), name);
  fields.push_back('=');
  if (quote) escapeValue(fields, fields.length(), value);
  else fields.append(value);
  fields.push_back(',');  // add a comma and pop it off later if required
}

const std::string& Point::toLineProtocol(const std::string& includeTags) {
  return createLineProtocol(includeTags);
}

const std::string& Point::createLineProtocol(const std::string& incTags,
                                             const bool excludeTimestamp) {
  return _data->createLineProtocol(incTags, excludeTimestamp);
}

const std::string& Point::Data::createLineProtocol(
    const std::string& incTags, const bool excludeTimestamp) {
  line.assign(measurement);
  if (!incTags.empty()) {
    line.push_back(',');
    line.append(incTags);
    line.pop_back();  // pop off the last comma
  }

  if (!tags.empty()) {
    line.push_back(',');
    line.append(tags);
    line.pop_back();
  }

  if (!fields.empty()) {
    line.push_back(' ');
    line.append(fields);
    line.pop_back();
  }

  if (!timeStamp.empty() && !excludeTimestamp) {
    line.push_back(' ');
    line.append(timeStamp);
  }

  line.push_back('\n');

  return line;
}

Point& Point::setTime(WritePrecision precision) {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  switch (precision) {
    case WritePrecision::NS:
      setTime(getTimeStamp(&tv, 9));
      break;
    case WritePrecision::US:
      setTime(getTimeStamp(&tv, 6));
      break;
    case WritePrecision::MS:
      setTime(getTimeStamp(&tv, 3));
      break;
    case WritePrecision::S:
      setTime(getTimeStamp(&tv, 0));
      break;
    case WritePrecision::NoTime:
      setTime("");
      break;
  }
  _data->tsWritePrecision = precision;
  return *this;
}

Point& Point::setTime(unsigned long long timestamp) {
  return setTime(timeStampToString(timestamp));
}

Point& Point::setTime(const std::string& timestamp) {
  _data->setTime(timestamp);
  return *this;
}

void Point::Data::setTime(const std::string& timestamp) {
  timeStamp.assign(timestamp);
}

Point& Point::clearFields() {
  _data->fields.clear();
  _data->timeStamp.clear();
  return *this;
}

Point& Point::clearTags() {
  _data->tags.clear();
  return *this;
}
