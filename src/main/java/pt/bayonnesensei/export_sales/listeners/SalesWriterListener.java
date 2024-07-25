package pt.bayonnesensei.export_sales.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import pt.bayonnesensei.export_sales.dto.SalesDTO;

import java.util.List;
import java.util.Objects;

@Component
@Slf4j
@RequiredArgsConstructor
public class SalesWriterListener implements ItemWriteListener<SalesDTO> {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterWrite(Chunk<? extends SalesDTO> items) {

        List<Long> itemWrittenIds = items.getItems()
                .stream()
                .map(SalesDTO::saleId)
                .toList();

        if (itemWrittenIds.isEmpty()) {
            log.info("No rows updated!!!!!!!!");
            return;
        }

        updateProcessedRecords(itemWrittenIds);
    }

    private void updateProcessedRecords(List<Long> itemWrittenIds) {
        Objects.requireNonNull(itemWrittenIds, "items written ids cannot be null");

        var sql = """
                UPDATE sales SET processed = true WHERE sale_id IN (:ids)
                """;

        var sqlParamSource = new MapSqlParameterSource("ids", itemWrittenIds);
        var namedParamJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);

        int totalRowsAffected = namedParamJdbcTemplate.update(sql, sqlParamSource);

        log.info("Total rows exported: {}", totalRowsAffected);
    }
}
