package com.luogh.learning.lab.disruptor.commponent;

import com.lmax.disruptor.EventFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public final class ValueEvent {

  private long value;

  public final static EventFactory<ValueEvent> EVENT_FACTORY = ValueEvent::new;
}