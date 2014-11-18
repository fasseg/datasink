/* 
 * Copyright 2014 FIZ Karlsruhe
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ROLE_ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */

package org.datasink.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.datasink.log.InputStreamLoggerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/**
 * Helper class for starting and monitoring a WeedFs Master node process
 */
public class SeaweedFsMaster {

    private static final Logger log = LoggerFactory.getLogger(SeaweedFsMaster.class);

    @Autowired
    Environment env;

    private Process masterProcess;

    private InputStreamLoggerTask loggerTask;

    @PostConstruct
    public void init() {
        if (env.getProperty("seaweedfs.master.enabled") != null &&
                !Boolean.parseBoolean(env.getProperty("seaweedfs.master.enabled"))) {
            // no seaweedfs master node is needed
            return;
        }
        /* check if the master dir exists and create if neccessary */
        final File dir = new File(env.getProperty("seaweedfs.master.dir"));
        if (!dir.exists()) {
            log.info("creating SeaweedFS master directory at " + dir.getAbsolutePath());
            if (!dir.mkdir()) {
                throw new IllegalArgumentException(
                        "Unable to create master directory. Please check the configuration");
            }
        }
        if (!dir.canRead() || !dir.canWrite()) {
            log.error("Unable to create master directory. The application was not initialiazed correctly");
            throw new IllegalArgumentException("Unable to use master directory. Please check the configuration");
        }
        if (env.getProperty("seaweedfs.binary") == null) {
            throw new IllegalArgumentException("The SeaweedFS Binary path has to be set");
        }
        final File binary = new File(env.getProperty("seaweedfs.binary"));
        if (!binary.exists()) {
            throw new IllegalArgumentException(new FileSystemNotFoundException(
                    "The seaweedfs binary can not be found at " + binary.getAbsolutePath()));
        }
        if (!binary.canExecute()) {
            throw new IllegalArgumentException("The SeaweedFS binary at " + binary.getAbsolutePath() +
                    " can not be executed");
        }
        try {

            final List<String> command = Arrays.asList(
                    env.getProperty("seaweedfs.binary"),
                    "master",
                    "-mdir=" + env.getProperty("seaweedfs.master.dir"),
                    "-port=" + env.getProperty("seaweedfs.master.port"),
                    "-ip=" + env.getProperty("seaweedfs.master.public")
                    );
            log.info("Starting SeaweedFS master with command '" + String.join(" ", command) + "'");
            masterProcess = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectInput(ProcessBuilder.Redirect.PIPE)
                    .start();

            final Executor executor = Executors.newSingleThreadExecutor();
            if (!masterProcess.isAlive()) {
                throw new IOException("SeaweedFS master could not be started! Exitcode " + masterProcess.exitValue());
            } else {
                log.info("SeaWeedFS master is running");
                executor.execute(new InputStreamLoggerTask(masterProcess.getInputStream()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isAvailable() {
        final File binary = new File(env.getProperty("seaweedfs.binary"));
        return binary.exists() && binary.canExecute();
    }

    public boolean isAlive() {
        return (masterProcess != null) && masterProcess.isAlive();
    }

    @PreDestroy
    public void shutdown() {
        log.info("shutting down SeaweedFS master");
        if (this.masterProcess != null && this.masterProcess.isAlive()) {
            this.masterProcess.destroy();
        }
    }
}
