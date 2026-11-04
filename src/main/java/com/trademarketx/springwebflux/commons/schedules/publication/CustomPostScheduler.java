package com.trademarketx.springwebflux.commons.schedules.publication;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import com.trademarketx.springwebflux.commons.conversion.EntityConversion;
import com.trademarketx.springwebflux.commons.schedules.AbstractCustomScheduler;
@Component
public class CustomPostScheduler<T, ID> extends AbstractCustomScheduler<T, ID> {
    public CustomPostScheduler(DatabaseClient databaseClient, EntityConversion entityConversion) {
        super(databaseClient, entityConversion); // The constructor AbstractCustomScheduler<T,ID>(DatabaseClient, EntityConversion) is undefined
    }
    @Override
    protected String getDateColumn() {
        return "scheduledDate";
    }
    @Override
    protected String getDateGetter() {
        return "getScheduledDate";
    }
}