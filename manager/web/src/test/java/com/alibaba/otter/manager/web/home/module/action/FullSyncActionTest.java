/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.home.module.action;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.citrus.turbine.dataresolver.FormGroup;

public class FullSyncActionTest {

    @Test
    public void rejectsMissingOrIncorrectConfirmation() {
        assertRejected(null);
        assertRejected("确定");
        assertRejected(" 确认 ");
    }

    @Test
    public void operationHandlersDoNotUseFormGroupValidation() {
        assertNoFormGroup("doCreate");
        assertNoFormGroup("doRetryStart");
    }

    private void assertRejected(String confirmation) {
        try {
            new FullSyncAction().doCreate(1L, confirmation, null);
            Assert.fail("invalid confirmation must be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("确认"));
        }
    }

    private void assertNoFormGroup(String methodName) {
        for (Method method : FullSyncAction.class.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                for (Annotation[] annotations : method.getParameterAnnotations()) {
                    for (Annotation annotation : annotations) {
                        Assert.assertFalse(methodName + " must rely on the pipeline CSRF check",
                                           annotation.annotationType().equals(FormGroup.class));
                    }
                }
                return;
            }
        }
        Assert.fail("missing action method " + methodName);
    }
}
