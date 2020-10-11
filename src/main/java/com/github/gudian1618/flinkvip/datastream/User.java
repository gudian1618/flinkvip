package com.github.gudian1618.flinkvip.datastream;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/10/11 4:12 下午
 */

public class User {

    private String id;
    private String name;
    private Integer age;
    private String gender;

    @Override
    public String toString() {
        return "User{" +
            "age=" + age +
            ", gender='" + gender + '\'' +
            '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
}
