package com.example.kafkarebalance

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaRebalanceDemoApplication

fun main(args: Array<String>) {
    runApplication<KafkaRebalanceDemoApplication>(*args)
}
