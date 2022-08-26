package com.victor.console.conf;

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
public class HiveConfigProperties {

    private String driverName;
    private String url;
    private String user;
    private String passWord;

    public HiveConfigProperties() {
    }

    public String getDriverName() {
        return this.driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassWord() {
        return this.passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }
}
