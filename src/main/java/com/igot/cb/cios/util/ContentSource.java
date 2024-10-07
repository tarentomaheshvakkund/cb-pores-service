package com.igot.cb.cios.util;

public enum ContentSource {
    CORNELL("${cornell.partner.code}"),
    UPGRAD("${upgrad.partner.code}");

    private String value;

    ContentSource(String value) {
        this.value = value;
    }

    public static ContentSource fromPartnerCode(String text) {
        for (ContentSource b : ContentSource.values()) {
            if (String.valueOf(b).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
