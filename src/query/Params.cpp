/**
 * 
 * Params.cpp: InfluxDB flux query param
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

#include "Params.h"
#include <algorithm>
#include <memory>

QueryParams::QueryParams() {
  _data = std::make_shared<ParamsList>();
}


QueryParams::QueryParams(const QueryParams& other) {
  _data = other._data;
}

 QueryParams::~QueryParams() {
   if(_data) {
    _data->clear();
   }
 }

QueryParams &QueryParams::operator=(const QueryParams &other) {
  if(this != &other) {
    _data = other._data;
  }
  return *this;
}

QueryParams &QueryParams::add(const std::string &name, float value, int decimalPlaces) {
  return add(name, (double)value, decimalPlaces);
}

QueryParams &QueryParams::add(const std::string &name, double value, int decimalPlaces) {
  return add(new FluxDouble(name, value, decimalPlaces));
}

QueryParams &QueryParams::add(const std::string &name, char value) {
  std::string s{value};
  return add(name, s);
}

QueryParams &QueryParams::add(const std::string &name, unsigned char value) {
  return add(name, (unsigned long)value);
}

QueryParams &QueryParams::add(const std::string &name, int value) {
  return add(name,(long)value);
}

QueryParams &QueryParams::add(const std::string &name, unsigned int value) {
  return add(name,(unsigned long)value);
}

QueryParams &QueryParams::add(const std::string &name, long value) {
    return add(new FluxLong(name, value));
}

QueryParams &QueryParams::add(const std::string &name, unsigned long value) {
  return add(new FluxUnsignedLong(name, value));
}

QueryParams &QueryParams::add(const std::string &name, bool value) {
  return add(new FluxBool(name, value));
}

QueryParams &QueryParams::add(const std::string &name, const std::string &value) {
  return add(name, value.c_str());
}

QueryParams &QueryParams::add(const std::string &name, const __FlashStringHelper *pstr) {
  std::unique_ptr<char[]> buff{ new char[strlen_P((PGM_P)pstr) + 1]};
  strcpy_P(buff.get(), (PGM_P)pstr);
  return add(name, buff.get());
}

QueryParams &QueryParams::add(const std::string &name, long long value)  {
  return add(name,(long)value);
}

QueryParams &QueryParams::add(const std::string &name, unsigned long long value) {
  return add(name,(unsigned long)value);
}

QueryParams &QueryParams::add(const std::string &name, const char *value) {
  return add(new FluxString(name, value, FluxDatatypeString));
}

QueryParams &QueryParams::add(const std::string &name, struct tm tm, unsigned long micros ) {
  return add(new FluxDateTime(name, FluxDatatypeDatetimeRFC3339Nano, tm, micros));
}

QueryParams &QueryParams::add(FluxBase *value) {
 if(_data) {
    _data->emplace_back(value);
  }
  return *this;
}

void QueryParams::remove(const std::string &name) {
  if(_data) {
    const auto& it = std::find_if(_data->begin(), _data->end(),
      [name](std::unique_ptr<FluxBase>& f) {
        return f->getRawValue() == name;
      });
    if(it != _data->end()) {
      _data->erase(it);
    }
  }
}

// Copy constructor
int QueryParams::size() {
  if(_data) {
    return _data->size();
  }
  return 0;
}

FluxBase *QueryParams::get(int i) {
  if(_data) {
    return _data->at(i).get();
  }
  return 0;
}

std::string QueryParams::jsonString(int i) {
  if(_data) {
    return _data->at(i)->jsonString();
  }
  return "";
}
