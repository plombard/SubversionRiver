package org.elasticsearch.river.subversion;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * Created by : Pascal.Lombard
 */
public class SubversionRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(SubversionRiver.class).asEagerSingleton();
    }
}
