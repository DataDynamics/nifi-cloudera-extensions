# NiFi Cloudera Hive Connection Pool & Processor

이 NAR는 Cloudera Hive JDBC Driver를 사용합니다.
Apache NiFi에 번들링 되어 있는 NAR는 Apache Hive JDBC Driver를 사용합니다.
Cloudera CDP 배포판에는 Apache Hive JDBC Driver가 번들링 되어 있지 않으므로 실행이 불가능합니다.
이러한 문제를 해결하기 위해서 
## Requirement

* Cloudera CFM 2.1.6 이상 (NiFi 1.26.0 이상)
  * CFM 2.1.6 : NiFi 1.26.0 
  * CFM 2.1.7 : NiFi 1.26.0 
* Apache Maven 3.8.0 이상
* JDK 11 이상

## Build

`<USER_HOME>/.m2/settings.xml` 파일을 다음과 같이 작성합니다.

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">

  <mirrors>
    <mirror>
      <id>central</id>
      <name>central</name>
      <url>http://nexus.opencloudengine.org/repository/maven-public/</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>
</settings>
```

NAR 파일 빌드는 다음과 같이 실행합니다.

```
# mvn clean package
```

## NAR 파일 배포

빌드한 NAR 파일을 배포하려면 다음과 같이 NAR 파일을 복사합니다.
소스코드를 수정하고 재배포하는 경우에도 동일하게 진행하면 변경내용을 확인하여 NiFi가 내부적으로 NAR를 재배포합니다.

```
# cp nifi-cloudera-processors-nar-1.0.0.nar /var/lib/nifi/extentions
# cd <NIFI_HOME>/bin
# sh nifi.sh start
```
