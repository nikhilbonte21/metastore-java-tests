package com.tests.main.tests.glossary.tests;


import javax.inject.Inject;
import java.util.List;

public class Main {

    private List<TestsMain> imageFileEditor;

    @Inject
    public Main(List<TestsMain> imageFileEditor) {
        this.imageFileEditor = imageFileEditor;
    }
}
