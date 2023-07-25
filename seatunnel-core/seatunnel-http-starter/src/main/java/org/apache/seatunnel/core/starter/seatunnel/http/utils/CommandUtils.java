package org.apache.seatunnel.core.starter.seatunnel.http.utils;

import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

public class CommandUtils {

    public static Integer exec(
            OutputStream outputStream, OutputStream errorOutputStream, String... command)
            throws InterruptedException, TimeoutException, IOException {
        return new ProcessExecutor()
                .command(command)
                .destroyOnExit()
                .redirectOutput(outputStream)
                .redirectError(errorOutputStream)
                .execute()
                .getExitValue();
    }
}
