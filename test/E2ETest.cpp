#include "E2ETest.h"

#include <string>

void E2ETest::run() {
    failures = 0;
    Serial.println("E2E Tests");
    testBuckets();   
    testWriteAndQuery(); 
    Serial.printf("E2E Tests %s\n", failures ? "FAILED" : "SUCCEEDED");
}

void E2ETest::testBuckets() {
    TEST_INIT("testBuckets");
    InfluxDBClient client(E2ETest::e2eApiUrl, E2ETest::orgName, E2ETest::bucketName, E2ETest::token);
    auto buckets = client.getBucketsClient();
    TEST_ASSERT(!buckets->isNull());
    TEST_ASSERTM(client.validateConnection(),client.getLastErrorMessage());
    std::string id = buckets->getOrgID("my-org");
    TEST_ASSERTM( isValidID(id.c_str()), id.length()?id:buckets->getLastErrorMessage());
    id = buckets->getOrgID("org");
    TEST_ASSERT( id == "");
    TEST_ASSERT(buckets->checkBucketExists("my-bucket"));

    TEST_ASSERT(!buckets->checkBucketExists("bucket-1"));
    Bucket b = buckets->createBucket("bucket-1");
    TEST_ASSERTM(!b.isNull(), buckets->getLastErrorMessage());
    TEST_ASSERTM(isValidID(b.getID()), b.getID());
    TEST_ASSERTM(!strcmp(b.getName(), "bucket-1"), b.getName());
    TEST_ASSERTM(b.getExpire() == 0, std::to_string(b.getExpire()));
    TEST_ASSERT(buckets->checkBucketExists("bucket-1"));
    TEST_ASSERT(buckets->deleteBucket(b.getID()));;
    TEST_ASSERT(!buckets->checkBucketExists("bucket-1"));
    TEST_ASSERT(!buckets->deleteBucket("bucket-1"));

    uint32_t monthSec = 3600*24*30;
    b = buckets->createBucket("bucket-2", monthSec);
    TEST_ASSERTM(!b.isNull(), buckets->getLastErrorMessage());
    TEST_ASSERT(buckets->checkBucketExists("bucket-2"));
    TEST_ASSERTM(b.getExpire() == monthSec, std::to_string(b.getExpire()));

    uint32_t yearSec = 12*monthSec;
    Bucket b2 = buckets->createBucket("bucket-3", yearSec);
    TEST_ASSERTM(!b2.isNull(), buckets->getLastErrorMessage());
    TEST_ASSERT(buckets->checkBucketExists("bucket-3"));
    TEST_ASSERTM(b2.getExpire() == yearSec, std::to_string(b2.getExpire()));

    TEST_ASSERT(buckets->checkBucketExists("bucket-2"));
    TEST_ASSERT(buckets->deleteBucket(b.getID()));
    TEST_ASSERT(buckets->checkBucketExists("bucket-3"));
    TEST_ASSERT(buckets->deleteBucket(b2.getID()));;
    TEST_ASSERT(!buckets->checkBucketExists("bucket-3"));
    TEST_ASSERT(!buckets->checkBucketExists("bucket-2"));

    TEST_END();
}

const char *QueryTemplate PROGMEM = "from(bucket: \"%s\") |> range(start: %ld) |> pivot(rowKey:[\"_time\"],columnKey: [\"_field\"],valueColumn: \"_value\") |> count(column: \"index\")";

void E2ETest::testWriteAndQuery() {
    TEST_INIT("testWriteAndQuery");
    const char *TestBucket = "test-bucket";
    InfluxDBClient client(E2ETest::e2eApiUrl, E2ETest::orgName, TestBucket, E2ETest::token);
    

    TEST_ASSERTM(client.validateConnection(),client.getLastErrorMessage());
    time_t start = time(nullptr);
    auto buckets = client.getBucketsClient();
    TEST_ASSERT(!buckets->isNull());
    Bucket t = buckets->findBucket(TestBucket);
    if(!t.isNull()) {
        Serial.println(F("Bucket already exists, deleting."));
        TEST_ASSERTM(buckets->deleteBucket(t.getID()), buckets->getLastErrorMessage());
    }
    TEST_ASSERT(!buckets->checkBucketExists(TestBucket));
    Bucket b = buckets->createBucket(TestBucket);
    TEST_ASSERTM(!b.isNull(), buckets->getLastErrorMessage());

    delay(1000);
    for (int i = 0; i < 5; i++) {
        std::unique_ptr<Point> p{createPoint("test1")};
        p->addField("index", i);
        TEST_ASSERTM(client.writePoint(*p.get()),client.getLastErrorMessage());
    }

    std::unique_ptr<char[]> query {
        new char[strlen_P(QueryTemplate) + strlen(TestBucket) + 10 + 1] };  // 10 - maximum=length of timestamp
    sprintf_P(query.get(), QueryTemplate, TestBucket, start);
    FluxQueryResult result = client.query(query.get());
    TEST_ASSERTM(result.next(), result.getError());
    FluxValue val = result.getValueByName("index");
    TEST_ASSERT(!val.isNull());
    TEST_ASSERTM(val.getLong() == 5, std::to_string(val.getLong()));
    TEST_ASSERTM(!result.next(), result.getError());
    
    result.close();

    TEST_ASSERT(buckets->deleteBucket(b.getID()));


    TEST_END();
}
