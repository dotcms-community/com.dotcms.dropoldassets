package com.dotcms.dropoldassets;


import com.dotmarketing.osgi.GenericBundleActivator;
import com.dotmarketing.util.Logger;
import com.dotmarketing.util.UtilMethods;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.core.util.CronExpression;
import org.osgi.framework.BundleContext;


public class Activator extends GenericBundleActivator {


    private final Runnable runner = new DropOldAssetsRunner();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    @Override
    public void start(final BundleContext context) throws Exception {
        boolean runNow = Props.getBool("RUN_ON_STARTUP", true);
        String jobCron = Props.getString("CRON_EXPRESSION");

        if (runNow) {
            scheduler.schedule(runner, 10, TimeUnit.SECONDS);
        }

        if (UtilMethods.isEmpty(jobCron)) {
            return;
        }
        CronExpression cron = new CronExpression(
                Props.getString("CRON_EXPRESSION"));  // Run at 6:15 in the morning

        Instant now = Instant.now();

        Instant previousRun = cron.getPrevFireTime(Date.from(now)).toInstant();
        Instant nextRun = cron.getNextValidTimeAfter(Date.from(previousRun)).toInstant();

        Duration delay = Duration.between(previousRun, now);
        Duration runEvery = Duration.between(previousRun, nextRun);

        Logger.info(this.getClass(),
                "Starting Delete Old Content Versions. Runs every:" + runEvery.getSeconds() + " seconds. Next run @ "
                        + nextRun);

        scheduler.scheduleAtFixedRate(runner, delay.getSeconds(), runEvery.getSeconds(), TimeUnit.SECONDS);


    }

    /**
     * Allows developers to correctly stop/un-register/remove Services and other utilities when an OSGi Plugin is
     * stopped.
     *
     * @param context The OSGi {@link BundleContext} object.
     * @throws Exception An error occurred during the plugin's stop.
     */
    @Override
    public void stop(final BundleContext context) throws Exception {

        Logger.info(this.getClass(), "Stopping Delete Old Content Versions");
        scheduler.shutdownNow();
    }


}
