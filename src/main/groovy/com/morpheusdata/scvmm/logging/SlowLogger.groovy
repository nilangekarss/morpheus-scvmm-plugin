package com.morpheusdata.scvmm.logging

import com.morpheusdata.scvmm.common.Constants
import com.morpheusdata.scvmm.tracker.TimeTracker
import groovy.transform.CompileStatic

/**
 * Custom logger class for SCVMM plugin.
 */
@CompileStatic
class SlowLogger {
    static final Integer SLOW_LOG_MAX_TIME = 5 * Constants.SECOND

    static void printLog(TimeTracker tracker, String activityName) {
        if (tracker.getOverallTime(activityName) > SLOW_LOG_MAX_TIME) {
            LogWrapper.instance.info("Activity ${activityName} took " +
                    "${tracker.getOverallTime(activityName) / Constants.SECOND}s to complete")
        } else {
            LogWrapper.instance.debug("Activity ${activityName} execution completed in " +
                    "${tracker.getOverallTime(activityName) / Constants.SECOND}s")
        }
    }

    static <T> T execute(String methodName, Closure<T> action) {
        LogWrapper.instance.debug("Entering ${methodName}")
        TimeTracker tracker = new TimeTracker(methodName)
        T result

        try {
            result = action.call()
        } finally {
            printLog(tracker.end(methodName), methodName)
            LogWrapper.instance.info("Exiting ${methodName}")
        }

        return result
    }
}
