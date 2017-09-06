/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting;

import java.io.Serializable;

/**
 * RpcResult.
 *
 * @author qian.lei
 */

public class MockResult implements Serializable {
    private static final long serialVersionUID = -3630485157441794463L;

    private final Object mResult;

    public MockResult(Object result) {
        mResult = result;
    }

    public Object getResult() {
        return mResult;
    }
}