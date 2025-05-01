package io.datadynamics.nifi.command;

import org.junit.Test;

import java.util.List;

public class ArgumentUtilsTest {

    @Test
    public void splitArgs() {
        List<String> strings = ArgumentUtils.splitArgs("-u;\"jdbc:hive2://hdu2.datalake.net:10000/default;principal=cloudera@DATALAKE\";-e;\"select 1\"", ';');
        System.out.println(strings);
    }

}