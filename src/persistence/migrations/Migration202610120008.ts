import { Migration } from '@mikro-orm/migrations';

/** O índice evita varrer a outbox ao buscar a correlação de origem da transação. */
export class Migration202610120008 extends Migration {
  override async up(): Promise<void> {
    this.addSql('CREATE INDEX outbox_transaction_origin_idx ON outbox_events(transaction_id,created_at,event_id)');
  }

  override async down(): Promise<void> {
    this.addSql('DROP INDEX outbox_transaction_origin_idx');
  }
}
