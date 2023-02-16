/**
 * 
 * TestSupport.cpp: Supporting functions for tests
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

#include <Arduino.h>
#if defined(ESP8266)
# include <ESP8266HTTPClient.h>
#elif defined(ESP32)
# include <HTTPClient.h>
#endif

#include <sstream>

#include "TestSupport.h"

static HTTPClient httpClient;

void printFreeHeap() {
  Serial.print("[TD] Free heap: ");  
  Serial.println(ESP.getFreeHeap());
}

int httpPOST(const std::string &url, std::string mess) {
  httpClient.setReuse(false);
  int code = 0;
  WiFiClient client;
  if(httpClient.begin(client, url.c_str())) {
    code = httpClient.POST(mess.c_str());
    httpClient.end();
  }
  return code;
}

int httpGET(const std::string &url) {
  httpClient.setReuse(false);
  int code = 0;
  WiFiClient client;
  if(httpClient.begin(client, url.c_str())) {
    code = httpClient.GET();
    if(code != 204) {
       //Serial.print("[TD] ");
       //std::string res = http.getString();
       //Serial.println(res);
    }
    httpClient.end();
  }
  return code;
}

bool deleteAll(const std::string &url) {
  if(WiFi.isConnected()) {
    return httpPOST(url + "/api/v2/delete","") == 204;
  }
  return false;
}

bool serverLog(const std::string &url, std::string mess) {
  if(WiFi.isConnected()) {
    return httpPOST(url + "/log", mess) == 204;
  } 
  return false;
}

bool isServerUp(const std::string &url) {
  if(WiFi.isConnected()) {
    return httpGET(url + "/status") == 200;
  }
  return false;
}


int countParts(const std::string &str, char separator) {
  return getParts(str, separator).size();
}

std::vector<std::string> getParts(const std::string &str, char separator) {
  std::vector<std::string> ret;
  std::stringstream ss(str);
  std::string word;
  while (!ss.eof()) {
    getline(ss, word, separator);
    ret.emplace_back(word);
  }
  return ret;
}

int countLines(FluxQueryResult flux) {
  int lines = 0;
  while(flux.next()) {
    lines++;
  }
  flux.close();
  return lines;
}

std::vector<std::string> getLines(FluxQueryResult flux) {
  std::vector<std::string> lines;
  while(flux.next()) {
    std::string line;
    int i=0;
    for(auto &val: flux.getValues()) {
      if(i>0) line += ",";
      line += val.getRawValue();
      i++;
    }
    lines.push_back(line);
  }
  flux.close();
  return lines;
}


bool compareTm(tm &tm1, tm &tm2) {
    time_t t1 = mktime(&tm1);
    time_t t2 = mktime(&tm2);
    return difftime(t1, t2) == 0;
} 

bool waitServer(const std::string &url, bool state) {
    int i = 0;
    while(isServerUp(url) != state && i<10 ) {
        if(!i) {
            Serial.println(state?"[TD] Starting server":"[TD] Shuting down server");
            httpGET(url + (state?"/start":"/stop"));
        }
        delay(500);
        i++;
    }
    return isServerUp(url) == state;
}