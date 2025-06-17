package com.b1uemusic.ads;

import org.apache.ibatis.annotations.Mapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.b1uemusic.ads.mapper")
public class AdsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdsApplication.class, args);
    }

}
