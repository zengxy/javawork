# javawork  odps code

maven repository：  http://maven.sdk.de.yushanfang.com/SNAPSHOT

mvn build command: mvn base:check mvn base:zip mvn base:submit

配置项：
base.program.type -  mapreduce  or  udf
base.endpoint -  http://api.sdk.de.yushanfang.com
base.ide.url -  http://ide.de.yushanfang.com
base.ide.resource.url -  http://@{env}.codebase.de.yushanfang.com/scheduler/res?id={rid}

# settings.xml   .m2

<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <pluginGroups>
        <pluginGroup>com.alibaba.base.plugins</pluginGroup>
    </pluginGroups>

    <profiles>
        <profile>
            <id>base</id>
            <pluginRepositories>
                <pluginRepository>
                    <id>base-snapshots</id>
                    <url>http://maven.sdk.de.yushanfang.com/SNAPSHOT</url>
                    <releases>
                        <enabled>false</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </pluginRepository>
            </pluginRepositories>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>base</activeProfile>
    </activeProfiles>
</settings>



