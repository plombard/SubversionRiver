/*
 * Copyright [2013] [Pascal Lombard]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.elasticsearch.river.subversion.crawler;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class ParametersTest {
    @Test
    public void testBuilder() throws Exception {
        Parameters parameters = new Parameters.ParametersBuilder().create();
        Assert.assertNotNull(parameters.getLogin().orNull());
        Assert.assertNotNull(parameters.getPassword().orNull());
        Assert.assertNotNull(parameters.getPath().orNull());
        Assert.assertNotNull(parameters.getStartRevision().orNull());
        Assert.assertFalse(parameters.getEndRevision().isPresent());
        Assert.assertFalse(parameters.getMaximumFileSize().isPresent());
        Assert.assertEquals(parameters.getPatternsToFilter(), ImmutableSet.<Pattern>of());
    }

    @Test
    public void testDefaultValues() throws Exception {
        Parameters parameters = new Parameters.ParametersBuilder().create();
        Assert.assertEquals(parameters.getLogin().get(), "anonymous");
        Assert.assertEquals(parameters.getPassword().get(), "password");
        Assert.assertEquals(parameters.getPath().get(), "/");
        Assert.assertEquals(parameters.getStartRevision().get(), Long.valueOf(1L));
        Assert.assertFalse(parameters.getEndRevision().isPresent());
        Assert.assertFalse(parameters.getMaximumFileSize().isPresent());
        Assert.assertEquals(parameters.getPatternsToFilter(), ImmutableSet.<Pattern>of());
    }
}
