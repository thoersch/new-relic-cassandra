package com.tylerhoersch.nr.cassandra;

public class JMXRequest {
    private final String domain;
    private final String name;
    private final String type;
    private final String scope;
    private final String attribute;

    private JMXRequest(String domain, String name, String type, String scope, String attribute) {
        this.domain = domain;
        this.name = name;
        this.type = type;
        this.scope = scope;
        this.attribute = attribute;
    }

    public String getDomain() {
        return domain;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getScope() {
        return scope;
    }

    public String getAttribute() {
        return attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JMXRequest that = (JMXRequest) o;

        if (domain != null ? !domain.equals(that.domain) : that.domain != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (scope != null ? !scope.equals(that.scope) : that.scope != null) return false;
        return !(attribute != null ? !attribute.equals(that.attribute) : that.attribute != null);

    }

    @Override
    public int hashCode() {
        int result = domain != null ? domain.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (scope != null ? scope.hashCode() : 0);
        result = 31 * result + (attribute != null ? attribute.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JMXRequest{" +
                "domain='" + domain + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", scope='" + scope + '\'' +
                ", attribute='" + attribute + '\'' +
                '}';
    }

    public static class JMXRequestBuilder {
        private String bDomain;
        private String bName;
        private String bType;
        private String bScope;
        private String bAttribute;

        public JMXRequestBuilder(final String domain) {
            this.bDomain = domain;
        }

        public JMXRequestBuilder name(final String name) { this.bName = name; return this; }
        public JMXRequestBuilder type(final String type) { this.bType = type; return this; }
        public JMXRequestBuilder scope(final String scope) { this.bScope = scope; return this; }
        public JMXRequestBuilder attribute(final String attribute) { this.bAttribute = attribute; return this; }
        public JMXRequest build() {
            return new JMXRequest(bDomain, bName, bType, bScope, bAttribute);
        }
    }
}
