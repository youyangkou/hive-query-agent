package com.victor.console.conf;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author Gerry
 * @date 2022-08-26
 */
@ConfigurationProperties(
        prefix = "hive-conf",
        ignoreUnknownFields = true,
        ignoreInvalidFields = true
)
@Component
@ToString
@Data
public class HiveConfigProperties {

    private String driverName;
    private String url;
    private String user;
    private String passWord;
    private String yarnQueuename;

    public HiveConfigProperties() {
    }
}
