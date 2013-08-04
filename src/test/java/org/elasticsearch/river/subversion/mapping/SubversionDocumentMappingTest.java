package org.elasticsearch.river.subversion.mapping;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Pascal.Lombard
 * 02/08/13 09:51
 */
public class SubversionDocumentMappingTest {
    @Test
    public void testGetInstance() throws Exception {

        final XContentBuilder instance = SubversionDocumentMapping.getInstance();
        System.out.println(instance.string());

        Assert.assertNotNull("Mapping for SubversionDocument must be set", instance);
    }
}
