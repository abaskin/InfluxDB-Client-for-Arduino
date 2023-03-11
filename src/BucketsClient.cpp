/**
 * 
 * BucketsClient.cpp: InfluxDB Buckets Client
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
#include "BucketsClient.h"
#include "util/helpers.h"

#include "util/debug.h"

static const char *propTemplate PROGMEM = "\"%s\":";
// Finds first id property from JSON response
enum class PropType {
  String,
  Number
};

static std::string findProperty(const char *prop,const std::string &json, PropType type = PropType::String);

static std::string findProperty(const char *prop,const std::string &json, PropType type) {
  INFLUXDB_CLIENT_DEBUG("[D] Searching for %s in %s\n", prop, json.c_str());
  int propLen = strlen_P(propTemplate)+strlen(prop)-2;
  std::unique_ptr<char[]> propSearch { new char[propLen+1] };
  sprintf_P(propSearch.get(), propTemplate, prop);
  auto i = json.find(propSearch.get());
  if (i != std::string::npos) {
    INFLUXDB_CLIENT_DEBUG("[D]   Found at %d\n", i);
    switch(type) {
      case PropType::String:
        i = json.find("\"", i + propLen);
        if (i != std::string::npos) {
          INFLUXDB_CLIENT_DEBUG("[D]    Found starting \" at %d\n", i);
          auto  e = json.find("\"", i + 1);
          if (e != std::string::npos) {
            INFLUXDB_CLIENT_DEBUG("[D]    Found ending \" at %d\n", e);
            return json.substr(i + 1, e);
          }
        }
      break;
      case PropType::Number:
        i = i+propLen;
        while(json[i] == ' ') {
          i++;
        }
        INFLUXDB_CLIENT_DEBUG("[D]    Found beginning of number at %d\n", i);
        auto e = json.find(",", i + 1);
        if (e != std::string::npos) {
          INFLUXDB_CLIENT_DEBUG("[D]    Found , at %d\n", e);
          return json.substr(i, e);
        }
      break;
    }
  }
  return "";
}

char *copyChars(const char *str) {
  char *ret = new char[strlen(str)+1];
  strcpy(ret, str);
  return ret;
}

Bucket::Bucket():_data(nullptr) {
}

Bucket::Bucket(const char *id, const char *name, const uint32_t expire) {
  _data = std::make_shared<Data>(id, name, expire);
}

Bucket::Bucket(const Bucket &other) {
    _data = other._data;
}

Bucket& Bucket::operator=(const Bucket& other) {
    if(this != &other) {
        _data = other._data;
    }
    return *this;
}

Bucket::~Bucket() {
}


Bucket::Data::Data(const char *id, const char *name, const uint32_t expire) {
  this->id = copyChars(id);
  this->name = copyChars(name);
  this->expire = expire;
}

Bucket::Data::~Data() {
}


const char *toStringTmplt PROGMEM = "Bucket: ID %s, Name %s, expire %u";
std::string Bucket::toString() const {
  int len = strlen_P(toStringTmplt) + (_data?!_data->name.empty():0) + (_data?_data->id.empty():0) + 10 + 1; //10 is maximum length of string representation of expire
  char *buff = new char[len];
  sprintf_P(buff, toStringTmplt, getID(), getName(), getExpire());
  std::string ret = buff;
  return ret;
}

BucketsClient::BucketsClient() {
  _data = nullptr;
}

BucketsClient::BucketsClient(ConnectionInfo *pConnInfo, HTTPService *service) {
  _data = std::make_shared<Data>(pConnInfo, service);
}

BucketsClient::BucketsClient(const BucketsClient &other) {
  _data = other._data;
}

BucketsClient &BucketsClient::operator=(const BucketsClient &other) {
  if(this != &other) {
        _data = other._data;
    }
    return *this;
}

BucketsClient &BucketsClient::operator=(std::nullptr_t) {
  _data = nullptr;
  return *this;
}

std::string BucketsClient::getOrgID(const char *org) {
  if(!_data) {
    return "";
  }
  if(isValidID(org)) {
    return org;
  }
  std::string url = _data->pService->getServerAPIURL();
  url += "orgs?org=";
  url += urlEncode(org);
  std::string id;
  INFLUXDB_CLIENT_DEBUG("[D] getOrgID: url %s\n", url.c_str());
  _data->pService->doGET(url.c_str(), 200, [&id](HTTPClient *client){
    id = findProperty("id",client->getString().c_str());
    return true;
  });
  return id;
}

bool BucketsClient::checkBucketExists(const char *bucketName) {
  Bucket b = findBucket(bucketName);
  return !b.isNull();
}

static const char *CreateBucketTemplate PROGMEM = "{\"name\":\"%s\",\"orgID\":\"%s\",\"retentionRules\":[{\"everySeconds\":%u}]}";

Bucket BucketsClient::createBucket(const char *bucketName, uint32_t expiresSec) {
  Bucket b;
  if(_data) {
    std::string orgID = getOrgID(_data->pConnInfo->org.c_str());
    
    if(!orgID.length()) {
      return b;
    }
    int expireLen = 0;
    uint32_t e = expiresSec; 
    do {
      expireLen++;
      e /=10;
    } while(e > 0);
    int len = strlen_P(CreateBucketTemplate) + strlen(bucketName) + orgID.length() + expireLen+1;
    std::unique_ptr<char[]> body { new char[len] };
    sprintf_P(body.get(), CreateBucketTemplate, bucketName, orgID.c_str(), expiresSec);
    std::string url = _data->pService->getServerAPIURL();
    url += "buckets";
    INFLUXDB_CLIENT_DEBUG("[D] CreateBucket: url %s, body %s\n", url.c_str(), body.get());
    _data->pService->doPOST(url.c_str(), body.get(), "application/json", 201, [&b](HTTPClient *client){
      std::string resp = client->getString().c_str();
      std::string id = findProperty("id", resp);
      std::string name = findProperty("name", resp);
      std::string expireStr = findProperty("everySeconds", resp, PropType::Number);
      uint32_t expire = strtoul(expireStr.c_str(), nullptr, 10);
      b = Bucket(id.c_str(), name.c_str(), expire);
      return true;
    });
  }
  return b;
}

bool BucketsClient::deleteBucket(const char *id) {
  if(!_data) {
    
    return false;
  }
  std::string url = _data->pService->getServerAPIURL();
  url += "buckets/";
  url += id;
  INFLUXDB_CLIENT_DEBUG("[D] deleteBucket: url %s\n", url.c_str());
  return _data->pService->doDELETE(url.c_str(), 204, nullptr);
}

Bucket BucketsClient::findBucket(const char *bucketName) {
  Bucket b;
  if(_data) {
    std::string url = _data->pService->getServerAPIURL();
    url += "buckets?name=";
    url += urlEncode(bucketName);
    INFLUXDB_CLIENT_DEBUG("[D] findBucket: url %s\n", url.c_str());
    _data->pService->doGET(url.c_str(), 200, [&b](HTTPClient *client){
      std::string resp { client->getString().c_str() };
      auto id { findProperty("id", resp) };
      if(id.length()) {
        std::string name = findProperty("name", resp);
        std::string expireStr = findProperty("everySeconds", resp, PropType::Number);
        uint32_t expire = strtoul(expireStr.c_str(), nullptr, 10);
        b = Bucket(id.c_str(), name.c_str(), expire);
      }
      return true;
    });
  }
  return b;
}
