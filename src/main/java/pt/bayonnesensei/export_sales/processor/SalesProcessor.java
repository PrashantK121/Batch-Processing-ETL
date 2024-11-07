package pt.bayonnesensei.export_sales.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import pt.bayonnesensei.export_sales.dto.SalesDTO;

@Component
@Slf4j
public class SalesProcessor implements ItemProcessor<SalesDTO, SalesDTO> {
    @Override
    public SalesDTO process(SalesDTO item) throws Exception {
        log.debug("processing the item: {}", item);
        if ("United States".equalsIgnoreCase(item.country())) {
            return null;
        }
        return item;
    }
}
