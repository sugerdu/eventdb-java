package org.osv.eventdb.util;

//import static org.junit.Assert.*;
//import static org.hamcrest.Matchers.*;
import java.util.*;
import org.junit.Test;

public class ConfigPropertiesTest {
	@Test
	public void testGetProperty() throws Exception {
		ConfigProperties config = new ConfigProperties();
		Set<String> keys = config.getPropertyNames();
		for (String key : keys)
			System.out.printf("%s=%s\n", key, config.getProperty(key));
	}
}