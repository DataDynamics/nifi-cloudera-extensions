# NiFi Cloudera Hive

## Requirement

* Cloudera CFM 2.1.6 이상 (NiFi 1.26.0 이상)
  * CFM 2.1.6 : NiFi 1.26.0 
  * CFM 2.1.7 : NiFi 1.26.0 
* Apache Maven 3.8.0 이상
* JDK 11 이상

## Build

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
