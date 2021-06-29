package org.apache.dubbo.qos.command.impl;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.qos.command.BaseCommand;
import org.apache.dubbo.qos.command.CommandContext;
import org.apache.dubbo.qos.command.annotation.Cmd;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.lang.reflect.Method;
import java.util.List;

@Cmd(name = "select", summary = "Select the index of the method you want to invoke", example = {
    "select [index]"
})
public class SelectTelnet implements BaseCommand {
    public static final AttributeKey<Boolean> SELECT_KEY = AttributeKey.valueOf("telnet.select");
    public static final AttributeKey<Method> SELECT_METHOD_KEY = AttributeKey.valueOf("telnet.select.method");

    private final InvokeTelnet invokeTelnet = new InvokeTelnet();

    @Override
    public String execute(CommandContext commandContext, String[] args) {
        if (args == null || args.length == 0) {
            return "Please input the index of the method you want to invoke, eg: \r\n select 1";
        }
        Channel channel = commandContext.getRemote();
        String message = args[0];
        List<Method> methodList = channel.attr(InvokeTelnet.INVOKE_METHOD_LIST_KEY).get();
        if (CollectionUtils.isEmpty(methodList)) {
            return "Please use the invoke command first.";
        }
        if (!StringUtils.isInteger(message) || Integer.parseInt(message) < 1 || Integer.parseInt(message) > methodList.size()) {
            return "Illegal index ,please input select 1~" + methodList.size();
        }
        Method method = methodList.get(Integer.parseInt(message) - 1);
        channel.attr(SELECT_METHOD_KEY).set(method);
        channel.attr(SELECT_KEY).set(Boolean.TRUE);
        String invokeMessage = channel.attr(InvokeTelnet.INVOKE_MESSAGE_KEY).get();
        return invokeTelnet.execute(commandContext, new String[]{invokeMessage});
    }
}
