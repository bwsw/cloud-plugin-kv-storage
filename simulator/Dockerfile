FROM cloudstack/simulator

ARG PLUGIN_PATH=https://oss.sonatype.org/service/local/artifact/maven/redirect?r=snapshots&g=com.bwsw&a=cloud-plugin-kv-storage&c=shaded&v=4.11.2.0-SNAPSHOT

ADD ["$PLUGIN_PATH", "/root/client/target/classes/META-INF/webapp/WEB-INF/lib/cloud-plugin-kv-storage.jar"]

COPY spring-event-bus-context.xml /root/client/target/classes/META-INF/cloudstack/core/spring-event-bus-context.xml
