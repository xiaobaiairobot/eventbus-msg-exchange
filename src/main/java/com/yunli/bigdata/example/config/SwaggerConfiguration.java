package com.yunli.bigdata.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfiguration {
  private static final String HEADER_CONFIGURATION = "springfox.documentation.header";

  @Value("${springfox.documentation.header}")
  private String header;

  @Bean
  public Docket createRestApi() {
    return new Docket(DocumentationType.SWAGGER_2)
        .apiInfo(new ApiInfoBuilder().title(header).version("1.0.0").build())
        .select()
        .apis(RequestHandlerSelectors.basePackage("com.yunli.bigdata"))
        .paths(PathSelectors.any())
        .build();
  }

  public String getHeader() {
    return header;
  }

  public void setHeader(String header) {
    this.header = header;
  }
}
