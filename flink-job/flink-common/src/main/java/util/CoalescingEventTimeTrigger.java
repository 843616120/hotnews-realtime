package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/** 事件时间定义窗口；处理时间只合并输出，空闲时也能输出最后一批窗口结果。 */
public class CoalescingEventTimeTrigger extends Trigger<JSONObject, TimeWindow> {
    private final ValueStateDescriptor<Long> nextEmit =
            new ValueStateDescriptor<>("next-coalesced-emit", Long.class);

    @Override
    public TriggerResult onElement(JSONObject value, long timestamp, TimeWindow window,
                                   TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        ValueState<Long> timer = ctx.getPartitionedState(nextEmit);
        if (timer.value() == null) {
            long at = ctx.getCurrentProcessingTime() + 1000;
            timer.update(at);
            ctx.registerProcessingTimeTimer(at);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        ValueState<Long> timer = ctx.getPartitionedState(nextEmit);
        if (!Long.valueOf(time).equals(timer.value())) return TriggerResult.CONTINUE;
        timer.clear();
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // WindowOperator 在清理定时器回调后删除状态，删除前补发最后的累计值。
        if (time != window.maxTimestamp() && time != window.maxTimestamp() + 3900000) {
            return TriggerResult.CONTINUE;
        }
        cancelOutputTimer(ctx);
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        cancelOutputTimer(ctx);
    }

    private void cancelOutputTimer(TriggerContext ctx) throws Exception {
        ValueState<Long> timer = ctx.getPartitionedState(nextEmit);
        Long at = timer.value();
        if (at != null) ctx.deleteProcessingTimeTimer(at);
        timer.clear();
    }
}
