package com.yidian.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountAndTimeTrigger<T> extends Trigger<T, TimeWindow> {


    private ReducingStateDescriptor<Long> countState;
    private ValueStateDescriptor<Long> timerState;
    private long timeSize;
    private long countSize;

    private CountAndTimeTrigger(long timeSize, long countSize) {
        this.timeSize = timeSize;
        this.countSize = countSize;
        this.countState =
                new ReducingStateDescriptor<>("countState", new Sum(), LongSerializer.INSTANCE);
        this.timerState =
                new ValueStateDescriptor<>("timerState", Long.class);
    }

    private CountAndTimeTrigger() {

    }

    public static <T> CountAndTimeTrigger<T> of(long timeSize, long countSize) {
        return new CountAndTimeTrigger<T>(timeSize, countSize);
    }

    @Override
    public TriggerResult onElement(T t, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        ValueState<Long> ts = triggerContext.getPartitionedState(timerState);
        ReducingState<Long> cs = triggerContext.getPartitionedState(countState);
        if (ts.value() == null) {
            long computeTime = triggerContext.getCurrentProcessingTime() + timeSize;
            ts.update(computeTime);
            triggerContext.registerProcessingTimeTimer(computeTime);
        }
        cs.add(1L);
        if (cs.get() >= countSize) {
            clearState(triggerContext);
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * @param l              定时器触发的时间戳
     * @param timeWindow     timeWindow.getEnd 比 timeWind.maxTimeStamp 大1毫秒
     * @param triggerContext 上下文
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        clearState(triggerContext);
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteProcessingTimeTimer(timeWindow.maxTimestamp());
        clearState(triggerContext);
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(countState);
        //window.getEnd-1=ctx.getCurrentProcessingTime() 窗口的触发时间
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    public void clearState(TriggerContext triggerContext) throws Exception {
        ValueState<Long> ts = triggerContext.getPartitionedState(timerState);
        ReducingState<Long> cs = triggerContext.getPartitionedState(countState);
        if (ts.value() != null) {
            triggerContext.deleteProcessingTimeTimer(ts.value());
        }
        ts.clear();
        cs.clear();
    }


    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }


}

