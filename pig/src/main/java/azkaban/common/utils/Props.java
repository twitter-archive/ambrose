/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import com.google.common.base.Objects;

/**
 * A less fucked properties class - Implements Map instead of extending HashMap
 * - Hash helpers for getting typed values - Support for getting with an
 * automatic default value - Throws exception if property is not defined
 * 
 * @author jay
 * 
 */
public class Props {

    private static Logger logger = Logger.getLogger(Props.class);

    private final Map<String, String> _current;
    private final Props _parent;

    public Props() {
        this._current = new ConcurrentHashMap<String, String>();
        this._parent = null;
    }

    public Props(Props parent) {
        this._current = new HashMap<String, String>();
        this._parent = parent;
    }

    public Props(Props parent, String... files) throws FileNotFoundException, IOException {
        this(parent, Arrays.asList(files));
    }

    public Props(Props parent, List<String> files) throws FileNotFoundException, IOException {
        this(parent);
        for(int i = 0; i < files.size(); i++) {
            InputStream input = new BufferedInputStream(new FileInputStream(new File(files.get(i)).getAbsolutePath()));
            loadFrom(input);
            input.close();
        }
    }

    public Props(Props parent, InputStream... inputStreams) throws IOException {
        this(parent);
        for(InputStream stream: inputStreams)
            loadFrom(stream);
    }

    private void loadFrom(InputStream inputStream) throws IOException {
        Properties properties = new Properties();
        properties.load(inputStream);
        this.put(properties);
    }

    public Props(Props parent, Map<String, String>... props) {
        this(parent);
        for(int i = props.length - 1; i >= 0; i--)
            this.putAll(props[i]);
    }

    public Props(Props parent, Properties... properties) {
        this(parent);
        for(int i = properties.length - 1; i >= 0; i--)
            this.put(properties[i]);
    }

    public Props(Props parent, Props props) {
        this(parent);
        if(props != null) {
            putAll(props);
        }
    }

    public static Props of(String... args) {
        return of((Props) null, args);
    }

    @SuppressWarnings("unchecked")
    public static Props of(Props parent, String... args) {
        if(args.length % 2 != 0)
            throw new IllegalArgumentException("Must have an equal number of keys and values.");
        Map<String, String> vals = new HashMap<String, String>(args.length / 2);
        for(int i = 0; i < args.length; i += 2)
            vals.put(args[i], args[i + 1]);
        return new Props(parent, vals);
    }

    public void clearLocal() {
        _current.clear();
    }

    /**
     * Check key in current Props then search in parent
     */
    public boolean containsKey(Object k) {
        return _current.containsKey(k) || (_parent != null && _parent.containsKey(k));
    }

    public boolean containsValue(Object value) {
        return _current.containsValue(value) || (_parent != null && _parent.containsValue(value));
    }

    /**
     * Return value if available in current Props otherwise return from parent
     */
    public String get(Object key) {
        if(_current.containsKey(key))
            return _current.get(key);
        else if(_parent != null)
            return _parent.get(key);
        else
            return null;
    }

    public Set<String> localKeySet() {
        return _current.keySet();
    }

    public Props getParent() {
        return _parent;
    }

    /**
     * Put the given string value for the string key. This method performs any
     * variable substitution in the value replacing any occurance of ${name}
     * with the value of get("name").
     * 
     * @param key The key to put the value to
     * @param value The value to do substitution on and store
     * 
     * @throws IllegalArgumentException If the variable given for substitution
     *         is not a valid key in this Props.
     */
    public String put(String key, String value) {
        return _current.put(key, value);
    }

    /**
     * Put the given Properties into the Props. This method performs any
     * variable substitution in the value replacing any occurrence of ${name}
     * with the value of get("name"). get() is called first on the Props and
     * next on the Properties object.
     * 
     * @param properties The properties to put
     * 
     * @throws IllegalArgumentException If the variable given for substitution
     *         is not a valid key in this Props.
     */
    public void put(Properties properties) {
        for(String propName: properties.stringPropertyNames()) {
            _current.put(propName, properties.getProperty(propName));
        }
    }

    public String put(String key, Integer value) {
        return _current.put(key, value.toString());
    }

    public String put(String key, Long value) {
        return _current.put(key, value.toString());
    }

    public String put(String key, Double value) {
        return _current.put(key, value.toString());
    }

    public void putAll(Map<? extends String, ? extends String> m) {
        if (m == null) {
            return;
        }
        
        for(Map.Entry<? extends String, ? extends String> entry: m.entrySet())
            this.put(entry.getKey(), entry.getValue());
    }

    public void putAll(Props p) {
        if(p == null) {
            return;
        }

        for(String key: p.getKeySet())
            this.put(key, p.get(key));
    }

    public void putLocal(Props p) {
        for(String key: p.localKeySet())
            this.put(key, p.get(key));
    }

    public String removeLocal(Object s) {
        return _current.remove(s);
    }

    /**
     * The number of unique keys defined by this Props and all parent Props
     */
    public int size() {
        return getKeySet().size();
    }

    /**
     * The number of unique keys defined by this Props (keys defined only in
     * parent Props are not counted)
     */
    public int localSize() {
        return _current.size();
    }

    public Class<?> getClass(String key) {
        try {
            if(containsKey(key))
                return Class.forName(get(key));
            else
                throw new UndefinedPropertyException("Missing required property '" + key + "'");
        } catch(ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Class<?> getClass(String key, Class<?> c) {
        if(containsKey(key))
            return getClass(key);
        else
            return c;
    }

    public String getString(String key, String defaultValue) {
        if(containsKey(key)) {
            return get(key);
        } else
            return defaultValue;
    }

    public String getString(String key) {
        if(containsKey(key))
            return get(key);
        else
            throw new UndefinedPropertyException("Missing required property '" + key + "'");
    }

    public List<String> getStringList(String key) {
        return getStringList(key, "\\s*,\\s*");
    }

    public List<String> getStringList(String key, String sep) {
        String val = get(key);
        if(val == null || val.trim().length() == 0)
            return Collections.emptyList();

        if(containsKey(key))
            return Arrays.asList(val.split(sep));
        else
            throw new UndefinedPropertyException("Missing required property '" + key + "'");
    }

    public List<String> getStringList(String key, List<String> defaultValue) {
        if(containsKey(key))
            return getStringList(key);
        else
            return defaultValue;
    }

    public List<String> getStringList(String key, List<String> defaultValue, String sep) {
        if(containsKey(key))
            return getStringList(key, sep);
        else
            return defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        if(containsKey(key))
            return "true".equalsIgnoreCase(get(key).trim());
        else
            return defaultValue;
    }

    public boolean getBoolean(String key) {
        if(containsKey(key))
            return "true".equalsIgnoreCase(get(key));
        else
            throw new UndefinedPropertyException("Missing required property '" + key + "'");
    }

    public long getLong(String name, long defaultValue) {
        if(containsKey(name))
            return Long.parseLong(get(name));
        else
            return defaultValue;
    }

    public long getLong(String name) {
        if(containsKey(name))
            return Long.parseLong(get(name));
        else
            throw new UndefinedPropertyException("Missing required property '" + name + "'");
    }

    public int getInt(String name, int defaultValue) {
        if(containsKey(name))
            return Integer.parseInt(get(name).trim());
        else
            return defaultValue;
    }

    public int getInt(String name) {
        if(containsKey(name))
            return Integer.parseInt(get(name).trim());
        else
            throw new UndefinedPropertyException("Missing required property '" + name + "'");
    }

    public double getDouble(String name, double defaultValue) {
        if(containsKey(name))
            return Double.parseDouble(get(name).trim());
        else
            return defaultValue;
    }

    public double getDouble(String name) {
        if(containsKey(name))
            return Double.parseDouble(get(name).trim());
        else
            throw new UndefinedPropertyException("Missing required property '" + name + "'");
    }

    public long getBytes(String name, long defaultValue) {
        if(containsKey(name))
            return getBytes(name);
        else
            return defaultValue;
    }

    public URI getUri(String name) {
        if(containsKey(name)) {
            try {
                return new URI(get(name));
            } catch(URISyntaxException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        } else {
            throw new UndefinedPropertyException("Missing required property '" + name + "'");
        }
    }

    public URI getUri(String name, URI defaultValue) {
        if(containsKey(name)) {
            return getUri(name);
        } else {
            return defaultValue;
        }
    }

    public URI getUri(String name, String defaultValue) {
        try {
            return getUri(name, new URI(defaultValue));
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if(o == this)
            return true;
        else if(o == null)
            return false;
        else if(o.getClass() != Props.class)
            return false;
        Props p = (Props) o;
        return _current.equals(p._current) && Objects.equal(this._parent, p._parent);
    }

    public boolean equalsProps(Props p) {
        if(p == null) {
            return false;
        }

        final Set<String> myKeySet = getKeySet();
        for(String s: myKeySet) {
            if(!get(s).equals(p.get(s))) {
                return false;
            }
        }

        return myKeySet.size() == p.getKeySet().size();
    }

    @Override
    public int hashCode() {
        int code = this._current.hashCode();
        if(_parent != null)
            code += _parent.hashCode();
        return code;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        for(Map.Entry<String, String> entry: this._current.entrySet()) {
            builder.append(entry.getKey());
            builder.append(": ");
            builder.append(entry.getValue());
            builder.append(", ");
        }
        if(_parent != null) {
            builder.append(" parent = ");
            builder.append(_parent.toString());
        }
        builder.append("}");
        return builder.toString();
    }

    public long getBytes(String name) {
        if(!containsKey(name))
            throw new UndefinedPropertyException("Missing required property '" + name + "'");

        String bytes = get(name);
        String bytesLc = bytes.toLowerCase().trim();
        if(bytesLc.endsWith("kb"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024;
        else if(bytesLc.endsWith("k"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024;
        else if(bytesLc.endsWith("mb"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024 * 1024;
        else if(bytesLc.endsWith("m"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024 * 1024;
        else if(bytesLc.endsWith("gb"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024 * 1024 * 1024;
        else if(bytesLc.endsWith("g"))
            return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024 * 1024 * 1024;
        else
            return Long.parseLong(bytes);
    }

    /**
     * Store only those properties defined at this local level
     * 
     * @param file The file to write to
     * @throws IOException If the file can't be found or there is an io error
     */
    public void storeLocal(File file) throws IOException {
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        try {
            storeLocal(out);
        } finally {
            out.close();
        }
    }

    @SuppressWarnings("unchecked")
    public Props local() {
        return new Props(null, _current);
    }

    /**
     * Store only those properties defined at this local level
     * 
     * @param out The output stream to write to
     * @throws IOException If the file can't be found or there is an io error
     */
    public void storeLocal(OutputStream out) throws IOException {
        Properties p = new Properties();
        for(String key: _current.keySet())
            p.setProperty(key, get(key));
        p.store(out, null);
    }

    /**
     * Returns a java.util.Properties file populated with the stuff in here.
     * 
     * @return
     */
    public Properties toProperties() {
        Properties p = new Properties();
        for(String key: _current.keySet())
            p.setProperty(key, get(key));

        return p;
    }

    /**
     * Store all properties, those local and also those in parent props
     * 
     * @param file The file to store to
     * @throws IOException If there is an error writing
     */
    public void storeFlattened(File file) throws IOException {
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        try {
            storeFlattened(out);
        } finally {
            out.close();
        }
    }

    /**
     * Store all properties, those local and also those in parent props
     * 
     * @param out The stream to write to
     * @throws IOException If there is an error writing
     */
    public void storeFlattened(OutputStream out) throws IOException {
        Properties p = new Properties();
        for(Props curr = this; curr != null; curr = curr.getParent())
            for(String key: curr.localKeySet())
                if(!p.containsKey(key))
                    p.setProperty(key, get(key));

        p.store(out, null);
    }

    /**
     * Get a map of all properties by string prefix
     * 
     * @param prefix The string prefix
     */
    public Map<String, String> getMapByPrefix(String prefix) {
        Map<String, String> values = new HashMap<String, String>();

        if(_parent != null) {
            for(Map.Entry<String, String> entry: _parent.getMapByPrefix(prefix).entrySet()) {
                values.put(entry.getKey(), entry.getValue());
            }
        }

        for(String key: this.localKeySet()) {
            if(key.startsWith(prefix)) {
                values.put(key.substring(prefix.length()), get(key));
            }
        }
        return values;
    }

    /**
     * @deprecated Replaced by {@link #getKeySet()}
     */
    @Deprecated 
    public Set<String> keySet() {
        return getKeySet();
    }
    
    public Set<String> getKeySet() {
        HashSet<String> keySet = new HashSet<String>();

        keySet.addAll(localKeySet());

        if(_parent != null) {
            keySet.addAll(_parent.getKeySet());
        }

        return keySet;
    }

    public void logProperties(String comment) {
        logger.info(comment);

        for(String key: getKeySet()) {
            logger.info("  key=" + key + " value=" + get(key));
        }
    }

    public static Props clone(Props p) {
        return copyNext(p);
    }

    private static Props copyNext(Props source) {
        Props priorNodeCopy = null;
        if(source.getParent() != null) {
            priorNodeCopy = copyNext(source.getParent());
        }
        Props dest = new Props(priorNodeCopy);
        for(String key: source.localKeySet()) {
            dest.put(key, source.get(key));
        }

        return dest;
    }

}
