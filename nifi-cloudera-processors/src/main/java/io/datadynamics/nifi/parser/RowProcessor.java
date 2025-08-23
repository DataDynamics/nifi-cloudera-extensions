package io.datadynamics.nifi.parser;

import java.util.List;

public interface RowProcessor {

    /**
     * 파싱된 1개 행(List<String>)을 가공하여 반환.
     * 반환된 결과가 즉시 출력 파일에 기록됩니다.
     */
    List<String> process(List<String> row);

}