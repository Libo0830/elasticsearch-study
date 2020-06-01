package com.libo.dto;

import java.io.Serializable;

/**
 * @FileName: Customer
 * @author: bli
 * @date: 2020年02月20日 14:15
 * @description:
 */
public class Customer implements Serializable {

    private String id;
    private String name;

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
