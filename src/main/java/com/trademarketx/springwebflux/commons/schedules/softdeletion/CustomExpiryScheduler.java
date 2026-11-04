package com.trademarketx.springwebflux.commons.schedules.softdeletion;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import com.trademarketx.springwebflux.commons.conversion.EntityConversion;
import com.trademarketx.springwebflux.commons.schedules.AbstractCustomScheduler;
@Component
public class CustomExpiryScheduler<T, ID> extends AbstractCustomScheduler<T, ID> {
    public CustomExpiryScheduler(DatabaseClient databaseClient, EntityConversion entityConversion) {
        super(databaseClient, entityConversion); // The constructor AbstractCustomScheduler<T,ID>(DatabaseClient, EntityConversion) is undefined
    }
    @Override
    protected String getDateColumn() {
        return "expiryDate";
    }
    @Override
    protected String getDateGetter() {
        return "getExpiryDate";
    }
}