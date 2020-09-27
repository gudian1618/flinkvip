package com.github.gudian1618.flinkvip.dataset;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/23 11:21 下午
 * 存储book数据的javabean
 */

public class Book {

    private String bookName;
    private String author;
    private String country;
    private String gender;
    private Integer age;

    @Override
    public String toString() {
        return "Book{" +
            "bookName='" + bookName + '\'' +
            ", author='" + author + '\'' +
            ", country='" + country + '\'' +
            ", gender='" + gender + '\'' +
            ", age=" + age +
            '}';
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
