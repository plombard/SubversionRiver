package org.elasticsearch.plugin.river.subversion;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.subversion.SubversionRiverModule;

/**
 * Created by : Pascal.Lombard
 */
public class SubversionRiverPlugin extends AbstractPlugin {

    @Inject public SubversionRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-subversion";
    }

    @Override
    public String description() {
        return "River Subversion Plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof RiversModule) {
            ((RiversModule) module).registerRiver("subversion", SubversionRiverModule.class);
        }
    }
}
