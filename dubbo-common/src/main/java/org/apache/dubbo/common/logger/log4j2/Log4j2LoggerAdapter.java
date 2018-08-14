/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.logger.log4j2;

import org.apache.dubbo.common.logger.Level;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class Log4j2LoggerAdapter implements LoggerAdapter {

    private File file;

    public Log4j2LoggerAdapter() {
        try {
            org.apache.logging.log4j.Logger logger = LogManager.getRootLogger();
            if (logger != null) {
                //Appender
                LoggerContext context = (LoggerContext)LogManager.getContext(false);
                Map<String, Appender> appenders = context.getConfiguration().getAppenders();
                Collection<Appender> appenderCollection = appenders.values();
                for (Appender appender : appenderCollection) {
                    if (appender instanceof FileAppender) {
                        FileAppender fileAppender = (FileAppender)appender;
                        String fileName = fileAppender.getFileName();
                        file = new File(fileName);
                        break;
                    }
                }
            }
        }catch (Throwable t){
        }
    }

    private static org.apache.logging.log4j.Level toLog4jLevel(Level level) {
        if (level == Level.ALL) { return org.apache.logging.log4j.Level.ALL; }
        if (level == Level.TRACE) { return org.apache.logging.log4j.Level.TRACE; }
        if (level == Level.DEBUG) { return org.apache.logging.log4j.Level.DEBUG; }
        if (level == Level.INFO) { return org.apache.logging.log4j.Level.INFO; }
        if (level == Level.WARN) { return org.apache.logging.log4j.Level.WARN; }
        if (level == Level.ERROR) { return org.apache.logging.log4j.Level.ERROR; }
        return org.apache.logging.log4j.Level.OFF;
    }

    private static Level fromLog4jLevel(org.apache.logging.log4j.Level level) {
        if (level == org.apache.logging.log4j.Level.ALL) { return Level.ALL; }
        if (level == org.apache.logging.log4j.Level.TRACE) { return Level.TRACE; }
        if (level == org.apache.logging.log4j.Level.DEBUG) { return Level.DEBUG; }
        if (level == org.apache.logging.log4j.Level.INFO) { return Level.INFO; }
        if (level == org.apache.logging.log4j.Level.WARN) { return Level.WARN; }
        if (level == org.apache.logging.log4j.Level.ERROR) { return Level.ERROR; }
        return Level.OFF;
    }

    @Override
    public Logger getLogger(Class<?> key) {
        return new Log4j2Logger(LogManager.getLogger(key));
    }

    @Override
    public Logger getLogger(String key) {
        return new Log4j2Logger(LogManager.getLogger(key));
    }

    @Override
    public Level getLevel() {
        return fromLog4jLevel(LogManager.getRootLogger().getLevel());
    }

    @Override
    public void setLevel(Level level) {
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public void setFile(File file) {
    }
}
