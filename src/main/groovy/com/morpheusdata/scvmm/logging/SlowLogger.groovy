package com.morpheusdata.scvmm.logging

class SlowLogger {
    def log
    long thresholdMs = 1000 // Log if execution takes longer than 1 second

    SlowLogger(def log, long thresholdMs = 1000) {
        this.log = log
        this.thresholdMs = thresholdMs
    }

    void logIfSlow(String methodName, long startTime, long endTime, Map opts = [:]) {
        long duration = endTime - startTime
        if (duration > thresholdMs) {
            log.warn("SLOW_EXECUTION: ${methodName} took ${duration} ms. opts: ${opts}")
        } else {
            log.debug("${methodName} executed in ${duration} ms.")
        }
    }
}
