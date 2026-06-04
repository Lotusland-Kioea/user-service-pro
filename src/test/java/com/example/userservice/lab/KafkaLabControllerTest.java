package com.example.userservice.lab;

import com.example.userservice.exception.GlobalExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * KafkaLabController 单元测试（MockMvc）。
 */
@WebMvcTest(KafkaLabController.class)
@Import(GlobalExceptionHandler.class)
@ActiveProfiles("test")
class KafkaLabControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaLabProducer producer;

    @MockBean
    private KafkaLabConsumer consumer;

    @BeforeEach
    void setUp() {
        when(consumer.getConsumedCount()).thenReturn(0L);
    }

    @Test
    @DisplayName("GET /kafka-lab/topics - 返回 Topic 信息")
    void testGetTopics() throws Exception {
        mockMvc.perform(get("/kafka-lab/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.user-events").exists())
                .andExpect(jsonPath("$.data.lab-messages").exists());
    }

    @Test
    @DisplayName("GET /kafka-lab/metrics - 返回统计信息")
    void testGetMetrics() throws Exception {
        when(producer.getMetrics()).thenReturn(new KafkaLabProducer.LabMetrics(10, 2));

        mockMvc.perform(get("/kafka-lab/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.success").value(10))
                .andExpect(jsonPath("$.data.failure").value(2));
    }

    @Test
    @DisplayName("POST /kafka-lab/send-batch/10 - 正常范围返回 200")
    void testSendBatchValid() throws Exception {
        when(producer.sendBatch(any(LabMessage.class), anyInt())).thenReturn(10L);

        mockMvc.perform(post("/kafka-lab/send-batch/10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.submitted").value(10));
    }
}
