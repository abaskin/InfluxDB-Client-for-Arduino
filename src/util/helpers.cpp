/**
 *
 * helpers.cpp: InfluxDB Client util functions
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
#include "helpers.h"

void timeSync(const char* tzInfo, const char* ntpServer1,
              const char* ntpServer2, const char* ntpServer3) {
  // Accurate time is necessary for certificate validation

  configTzTime(tzInfo, ntpServer1, ntpServer2, ntpServer3);

  // Wait till time is synced
  Serial.print("Syncing time");
  int i = 0;
  while (time(nullptr) < 1000000000l && i < 40) {
    Serial.print(".");
    delay(500);
    i++;
  }
  Serial.println();

  // Show time
  time_t tnow = time(nullptr);
  Serial.printf("Synchronized time: %s\n", ctime(&tnow));
}

unsigned long long getTimeStamp(struct timeval* tv, const int secFracDigits) {
  switch (secFracDigits) {
    case 0:
      return tv->tv_sec;
    case 6:
      return tv->tv_sec * 1000000LL + tv->tv_usec;
    case 9:
      return tv->tv_sec * 1000000000LL + tv->tv_usec * 1000LL;
    case 3:
    default:
      return tv->tv_sec * 1000LL + tv->tv_usec / 1000LL;
  }
}

std::string timeStampToString(const unsigned long long timestamp,
                              const int extraCharsSpace) {
  // 22 is max long long string length (18)
  char buff[22 + extraCharsSpace + 1];
  snprintf(buff, 22, "%llu", timestamp);
  return buff;
}

constexpr char escapeChars[] = "=\r\n\t ,";

size_t escapeKey(std::string& str, const size_t start, const std::string& key,
                 const bool escapeEqual) {
  std::string ret;
  for (auto c : key) {
    if (strchr(escapeEqual ? escapeChars : escapeChars + 1, c)) {
      ret.push_back('\\');
    }
    ret.push_back(c);
  }
  str.insert(start, ret);
  return ret.length();
}

size_t escapeValue(std::string& str, const size_t start,
                   const std::string& value) {
  std::string ret;
  ret.push_back('"');
  for (auto c : value) {
    switch (c) {
      case '\\':
      case '\"':
        ret.push_back('\\');
        break;
    }
    ret.push_back(c);
  }
  ret.push_back('"');
  str.insert(start, ret);
  return ret.length();
}

constexpr char invalidChars[] = "$&+,/:;=?@ <>#%{}|\\^~[]`";

static char hex_digit(char c) { return "0123456789ABCDEF"[c & 0x0F]; }

std::string urlEncode(const std::string& src) {
  std::string ret;
  for (auto c : src) {
    switch (strchr(invalidChars, c) != nullptr) {
      case true:
        ret.push_back('%');
        ret.push_back(hex_digit(c >> 4));
        ret.push_back(hex_digit(c));
        break;
      case false:
        ret.push_back(c);
        break;
    }
  }
  return ret;
}

bool isValidID(const std::string& idString) {
  return idString.length() == 16 &&
         idString.find_first_not_of("0123456789abcdefABCDEF") ==
             std::string::npos;
}

const char* bool2string(const bool val) { return (val ? "true" : "false"); }

size_t getNumLength(const long long l) { return std::to_string(l).length(); }

bool endsWith(const std::string str, const std::string suffix) {
  return str.size() >= suffix.size() &&
         0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
}

bool startsWith(const std::string str, const std::string prefix) {
  return str.size() >= prefix.size() &&
         0 == str.compare(0, prefix.size(), prefix);
}

void trim(std::string& str) {
  str.erase(std::find_if(str.rbegin(), str.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            str.end());
}