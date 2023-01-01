/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.kafka.cmd;

import java.util.Arrays;

import org.apache.jena.Jena;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Version;

public class fk {

    public static void main(String[] args) {
        if ( args.length < 2 ) {
            System.err.println("No subcommand");
            System.exit(1);
        }

        if ( args.length == 0 ) {
            System.err.println("Usage: fk SUB ARGS...");
            System.exit(1);
        }

        String cmd = args[0];
        String[] argsSub = Arrays.copyOfRange(args, 1, args.length);
        String cmdExec = cmd;

        // Help
        switch (cmdExec) {
            case "help" :
            case "-h" :
            case "-help" :
            case "--help" :
                System.err.println("Commands: send, dump");
                return;
            case "version":
            case "--version":
            case "-version":
                version();
                System.exit(0);
        }

        // Map to full name.
        switch (cmdExec) {
            case "s": cmdExec = "send"; break;
            case "d": cmdExec = "dump"; break;
        }

        // Execute sub-command
        switch (cmdExec) {
            case "send":  FK_Send.main(argsSub); break;
            case "dump":  FK_DumpTopic.main(argsSub); break;
            default:
                System.err.println("Failed to find a command match for '"+cmd+"'");
        }
    }



    private static void version() {
        Version version = new Version();
        version.addClass(Jena.class) ;
        version.print(IndentedWriter.stdout);
        System.exit(0) ;
    }
}
