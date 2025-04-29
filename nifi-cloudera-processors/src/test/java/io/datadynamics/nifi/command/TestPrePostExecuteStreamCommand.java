package io.datadynamics.nifi.command;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Test only runs on *nix")
public class TestPrePostExecuteStreamCommand {

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    @Test
    public void testKerberos() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestLogStdErr.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1mb.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(PrePostExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(PrePostExecuteStreamCommand.PRE_COMMAND, "echo hello");
        controller.setProperty(PrePostExecuteStreamCommand.POST_COMMAND, "echo world");
        controller.setProperty(PrePostExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(PrePostExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
    }

}