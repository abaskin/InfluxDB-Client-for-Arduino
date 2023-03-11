
#include "HTTPService.h"
#include "Platform.h"
#include "Version.h"

#include "util/debug.h"

static const char UserAgent[] PROGMEM = "influxdb-client-arduino/" INFLUXDB_CLIENT_VERSION " (" INFLUXDB_CLIENT_PLATFORM " " INFLUXDB_CLIENT_PLATFORM_VERSION ")";

#if defined(ESP8266)         
bool checkMFLN(BearSSL::WiFiClientSecure  *client, std::string url);
#endif

// This cannot be put to PROGMEM due to the way how it is used
static const char *RetryAfter = "Retry-After";
const char *TransferEncoding = "Transfer-Encoding";

HTTPService::HTTPService(ConnectionInfo *pConnInfo):_pConnInfo(pConnInfo) {
  _apiURL = pConnInfo->serverUrl;
  _apiURL += "/api/v2/";
  auto https{pConnInfo->serverUrl.find("https") != std::string::npos};
  if(https) {
#if defined(ESP8266)         
    BearSSL::WiFiClientSecure *wifiClientSec = new BearSSL::WiFiClientSecure;
    if (pConnInfo->insecure) {
      wifiClientSec->setInsecure();
    } else if(pConnInfo->certInfo && strlen_P(pConnInfo->certInfo) > 0) {
      if(strlen_P(pConnInfo->certInfo) > 60 ) { //differentiate fingerprint and cert
         _cert.reset(new BearSSL::X509List(pConnInfo->certInfo)); 
         wifiClientSec->setTrustAnchors(_cert.get());
      } else {
         wifiClientSec->setFingerprint(pConnInfo->certInfo);
      }
    }
    checkMFLN(wifiClientSec, pConnInfo->serverUrl);
#elif defined(ESP32)
    WiFiClientSecure *wifiClientSec = new WiFiClientSecure;  
    if (pConnInfo->insecure) {
#ifndef ARDUINO_ESP32_RELEASE_1_0_4
      // This works only in ESP32 SDK 1.0.5 and higher
      wifiClientSec->setInsecure();
#endif            
    } else if(pConnInfo->certInfo && strlen_P(pConnInfo->certInfo) > 0) { 
      wifiClientSec->setCACert(pConnInfo->certInfo);
    }
#endif    
    _wifiClient.reset(wifiClientSec);
  } else {
    _wifiClient.reset(new WiFiClient);
  }
  if(!_httpClient) {
    _httpClient.reset(new HTTPClient);
  }
  _httpClient->setReuse(_httpOptions._connectionReuse);

  _httpClient->setUserAgent(FPSTR(UserAgent));
};

HTTPService::~HTTPService() {
}


void HTTPService::setHTTPOptions(const HTTPOptions & httpOptions) {
    _httpOptions = httpOptions;
    if(!_httpClient) {
        _httpClient.reset(new HTTPClient);
    }
    _httpClient->setReuse(_httpOptions._connectionReuse);
    _httpClient->setTimeout(_httpOptions._httpReadTimeout);
#if defined(ESP32) 
     _httpClient->setConnectTimeout(_httpOptions._httpReadTimeout);
#endif
}

// parse URL for host and port and call probeMaxFragmentLength
#if defined(ESP8266)         
bool checkMFLN(BearSSL::WiFiClientSecure  *client, std::string url) {
    auto index = url.find(':');
    if (index == std::string::npos) {
        return false;
    }
    std::string protocol = url.substr(0, index);
    int port = -1;
    url.erase(0, (index + 3)); // remove http:// or https://

    if (protocol == "http") {
        // set default port for 'http'
        port = 80;
    } else if (protocol == "https") {
        // set default port for 'https'
        port = 443;
    } else {
        return false;
    }
    index = url.find("/");
    std::string host = url.substr(0, index);
    url.erase(0, index); // remove host 
    // check Authorization
    index = host.find("@");
    if(index >= 0) {
        host.erase(0, index + 1); // remove auth part including @
    }
    // get port
    index = host.find(":");
    if(index >= 0) {
        std::string portS = host;
        host = host.substr(0, index); // hostname
        portS.erase(0, (index + 1)); // remove hostname + :
        port = std::atoi(portS.c_str()); // get port
    }
    INFLUXDB_CLIENT_DEBUG("[D] probeMaxFragmentLength to %s:%d\n", host.c_str(), port);
    bool mfln = client->probeMaxFragmentLength(host.c_str(), port, 1024);
    INFLUXDB_CLIENT_DEBUG("[D]  MFLN:%s\n", mfln ? "yes" : "no");
    if (mfln) {
        client->setBufferSizes(1024, 1024);
    } 
    return mfln;
}
#endif //ESP8266

bool HTTPService::beforeRequest(const char *url) {
   if(!_httpClient->begin(*_wifiClient, url)) {
    _pConnInfo->lastError = "begin failed";
    return false;
  }
  if(_pConnInfo->authToken.length() > 0) {
    _httpClient->addHeader(F("Authorization"), 
                           "Token " + String(_pConnInfo->authToken.c_str()));
  }
  const char * headerKeys[] = {RetryAfter, TransferEncoding} ;
  _httpClient->collectHeaders(headerKeys, 2);
  return true;
}

bool HTTPService::doPOST(const char *url, const char *data, const char *contentType, int expectedCode, httpResponseCallback cb) {
  INFLUXDB_CLIENT_DEBUG("[D] POST request - %s, data: %d bytes, type %s\n", url, strlen(data), contentType);
  if(!beforeRequest(url)) {
    return false;
  }
  if(contentType) {
    _httpClient->addHeader(F("Content-Type"), FPSTR(contentType));
  }
  _lastStatusCode = _httpClient->POST((uint8_t *) data, strlen(data));
  return afterRequest(expectedCode, cb);
}

bool HTTPService::doPOST(const char *url, Stream *stream, const char *contentType, int expectedCode, httpResponseCallback cb) {
  INFLUXDB_CLIENT_DEBUG("[D] POST request - %s, data: %d bytes, type %s\n", url, stream->available(), contentType);
  if(!beforeRequest(url)) {
    return false;
  }
  if(contentType) {
    _httpClient->addHeader(F("Content-Type"), FPSTR(contentType));
  }
  _lastStatusCode = _httpClient->sendRequest("POST", stream, stream->available());
  return afterRequest(expectedCode, cb);
}

bool HTTPService::doGET(const char *url, int expectedCode, httpResponseCallback cb) {
  INFLUXDB_CLIENT_DEBUG("[D] GET request - %s\n", url);
  if(!beforeRequest(url)) {
    return false;
  }
  _lastStatusCode = _httpClient->GET();
  return afterRequest(expectedCode, cb, false);
}

bool HTTPService::doDELETE(const char *url, int expectedCode, httpResponseCallback cb) {
  INFLUXDB_CLIENT_DEBUG("[D] DELETE - %s\n", url);
  if(!beforeRequest(url)) {
    return false;
  }
  _lastStatusCode = _httpClient->sendRequest("DELETE");
  return afterRequest(expectedCode, cb, false);
}

bool HTTPService::afterRequest(int expectedStatusCode, httpResponseCallback cb,  bool modifyLastConnStatus) {
    if(modifyLastConnStatus) {
        _lastRequestTime = millis();
        INFLUXDB_CLIENT_DEBUG("[D] HTTP status code - %d\n", _lastStatusCode);
        _lastRetryAfter = 0;
        if(_lastStatusCode >= 429) { //retryable server errors
            if(_httpClient->hasHeader(RetryAfter)) {
                _lastRetryAfter = _httpClient->header(RetryAfter).toInt();
                INFLUXDB_CLIENT_DEBUG("[D] Reply after - %d\n", _lastRetryAfter);
            }
        }
    }
    _pConnInfo->lastError.clear();
    bool ret = _lastStatusCode == expectedStatusCode;
    bool endConnection = true;
    if(!ret) {
        if(_lastStatusCode > 0) {
            _pConnInfo->lastError = _httpClient->getString().c_str();
            INFLUXDB_CLIENT_DEBUG("[D] Response:\n%s\n", _pConnInfo->lastError.c_str());
        } else {
            _pConnInfo->lastError = _httpClient->errorToString(_lastStatusCode).c_str();
            INFLUXDB_CLIENT_DEBUG("[E] Error - %s\n", _pConnInfo->lastError.c_str());
        }
    } else if(cb){
      endConnection = cb(_httpClient.get());
    }
    if(endConnection) {
        _httpClient->end();
    }
    return ret;
}