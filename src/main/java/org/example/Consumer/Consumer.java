package org.example.Consumer;

import org.example.Message;

import java.util.List;

public interface Consumer {
    List<Message> poll();
}
