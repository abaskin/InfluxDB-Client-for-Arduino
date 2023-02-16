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

#include "Point.h"
#include "util/helpers.h"

Point::Point(const std::string & measurement)
{
  _data = std::make_shared<Data>(escapeKey(measurement, false));
}

Point::~Point() {
}

Point::Data::Data(const std::string &_measurement) : measurement(_measurement) {
  timestamp = "";
  tsWritePrecision = WritePrecision::NoTime;
}

Point::Data::~Data() {}

Point::Point(const Point &other) {
  *this = other;
}

Point& Point::operator=(const Point &other) {
  if(this != &other) {
    this->_data = other._data;
  }
  return *this;
}

void Point::addTag(const std::string &name, std::string value) {
  if(_data->tags.length() > 0) {
      _data->tags += ',';
  }
  _data->tags += escapeKey(name);
  _data->tags += '=';
  _data->tags += escapeKey(value);
}

void Point::addField(const std::string &name, long long value) {
  char buff[23];
  snprintf(buff, 50, "%lld", value);
  putField(name, std::string(buff)+"i");
}

void Point::addField(const std::string &name, unsigned long long value) {
  char buff[23];
  snprintf(buff, 50, "%llu", value);
  putField(name, std::string(buff)+"i");
}

void Point::addField(const std::string &name, const char *value) { 
    putField(name, escapeValue(value)); 
}

void Point::addField(const std::string &name, const __FlashStringHelper *pstr) {
    std::unique_ptr<char[]> buff{new char[strlen_P((PGM_P)pstr) + 1]};
    addField(name, buff.get());
}

void Point::addField(const std::string &name, float value, int decimalPlaces) { 
    if(!isnan(value)) putField(name, std::to_string(value)); 
}

void Point::addField(const std::string &name, double value, int decimalPlaces) {
    if(!isnan(value)) putField(name, std::to_string(value)); 
}

void Point::addField(const std::string &name, char value) { 
    addField(name, std::string{value}); 
}

void Point::addField(const std::string &name, unsigned char value) {
    putField(name, std::to_string(value) + "i");
}

void Point::addField(const std::string &name, int value) { 
    putField(name, std::to_string(value) + "i"); 
}

void Point::addField(const std::string &name, unsigned int value) { 
    putField(name, std::to_string(value) + "i"); 
}

void Point::addField(const std::string &name, long value)  { 
    putField(name, std::to_string(value) + "i"); 
}

void Point::addField(const std::string &name, unsigned long value) { 
    putField(name, std::to_string(value) + "i"); 
}

void Point::addField(const std::string &name, bool value)  { 
    putField(name, bool2string(value)); 
}

void Point::addField(const std::string &name, const std::string &value)  { 
    addField(name, value); 
}

void Point::putField(const std::string &name, const std::string &value) {
    if(_data->fields.length() > 0) {
        _data->fields += ',';
    }
    _data->fields += escapeKey(name);
    _data->fields += '=';
    _data->fields += value;
}

std::string Point::toLineProtocol(const std::string &includeTags) const {
    return createLineProtocol(includeTags);
}

std::string Point::createLineProtocol(const std::string &incTags, bool excludeTimestamp) const {
    std::string line;
    line.reserve(
        _data->measurement.length() + 1 + incTags.length() + 1 + 
        _data->tags.length() + 1 + _data->fields.length() + 1 + 
        _data->timestamp.length());
    line += _data->measurement;
    if(incTags.length()>0) {
        line += ",";
        line += incTags;
    }
    if(hasTags()) {
        line += ",";
        line += _data->tags;
    }
    if(hasFields()) {
        line += " ";
        line += _data->fields;
    }
    if(hasTime() && !excludeTimestamp) {
        line += " ";
        line += _data->timestamp;
    }
    return line;
 }

void Point::setTime(WritePrecision precision) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    
    switch(precision) {
        case WritePrecision::NS:
            setTime(getTimeStamp(&tv,9));
            break;
        case WritePrecision::US:
            setTime(getTimeStamp(&tv,6));
            break;
        case WritePrecision::MS: 
            setTime(getTimeStamp(&tv,3));
            break;
        case WritePrecision::S:
            setTime(getTimeStamp(&tv,0));
            break;
        case WritePrecision::NoTime:
            setTime((char *)nullptr);
            break;
    }
    _data->tsWritePrecision = precision;
}

void  Point::setTime(unsigned long long timestamp) {
    setTime(timeStampToString(timestamp));
}

void Point::setTime(const std::string &timestamp) {
    _data->timestamp = timestamp;
}

void Point::setTime(const char *timestamp) {
    _data->timestamp = timestamp;   
}

void Point::setTime(char *timestamp) {
    _data->timestamp = timestamp;
}

void  Point::clearFields() {
    _data->fields = (char *)nullptr;
    _data->timestamp = "";
}

void Point:: clearTags() {
    _data->tags = (char *)nullptr;
}
