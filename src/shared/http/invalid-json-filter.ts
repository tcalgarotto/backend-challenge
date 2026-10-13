import { BadRequestException, Catch, type ArgumentsHost, type ExceptionFilter } from '@nestjs/common';
import { requestCorrelationId, type HttpResponse } from './typed-response.js';

/** Falha do parser ocorre antes do controlador, mas deve preservar o contrato de entrada. */
@Catch(BadRequestException)
export class InvalidJsonFilter implements ExceptionFilter {
  catch(_error: BadRequestException, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<HttpResponse>();
    response.status(400).json({
      error: { code: 'INVALID_PAYLOAD', message: 'INVALID_PAYLOAD', retryable: false },
      correlationId: requestCorrelationId(response),
    });
  }
}
