package com.luogh.learning.lab.flink;

import com.google.common.collect.Sets;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class WindowWordCountExec {

  public static void main(String[] args) throws Exception {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("yarn.resourcemanager.address", "172.23.7.138:8032");
    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConfiguration);
    yarnClient.start();

    List<ApplicationReport> applications = yarnClient
        .getApplications(Sets.newHashSet("Apache Flink"), EnumSet.of(
            YarnApplicationState.RUNNING));

    for (ApplicationReport report : applications) {
      System.out.println("application id: " + report.getApplicationId().toString());
      System.out.println("application state: " + report.getYarnApplicationState().toString());
      System.out.println("application type: " + report.getApplicationType());
      System.out.println("application name: " + report.getName());
      System.out.println("application tracking url: " + report.getTrackingUrl());
      System.out.println("application original tracking url: " + report.getOriginalTrackingUrl());
      System.out.println("application queue: " + report.getQueue());
    }

  }
}
