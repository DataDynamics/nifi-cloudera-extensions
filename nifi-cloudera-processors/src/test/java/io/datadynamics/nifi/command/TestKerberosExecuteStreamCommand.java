package io.datadynamics.nifi.command;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Test only runs on *nix")
public class TestKerberosExecuteStreamCommand {

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    @Test
    public void testKerberos() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestLogStdErr.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1mb.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(KerberosExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(KerberosExecuteStreamCommand.KERBEROS_PRINCIPAL, "cloudera@datalake.ibk.co.kr");
        controller.setProperty(KerberosExecuteStreamCommand.KERBEROS_KEYTAB_PATH, "/opt/cloudera/security/cloudera.keytab");
        controller.setProperty(KerberosExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(KerberosExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
//        controller.assertTransferCount(KerberosExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
//        controller.assertTransferCount(KerberosExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
//        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(KerberosExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
//        MockFlowFile flowFile = flowFiles.get(0);
//        assertEquals(0, flowFile.getSize());
//        assertTrue(flowFile.getAttribute("execution.error").contains("fffffffffffffffffffffffffffffff"));
    }
}
