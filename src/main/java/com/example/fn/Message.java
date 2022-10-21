package com.example.fn;
public class Message {
    
    private String streamName;
        private String compartmentOCID;
        private String key;
        private String value;

        public String getStreamName() {
            return streamName;
        }

        public void setStreamName(String streamName) {
            this.streamName = streamName;
        }

        public String getCompartmentOCID() {
            return compartmentOCID;
        }

        public void setCompartmentOCID(String compartmentOCID) {
            this.compartmentOCID = compartmentOCID;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
}
