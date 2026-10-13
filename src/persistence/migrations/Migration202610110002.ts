import { Migration } from '@mikro-orm/migrations';
import { financialUp, financialDown } from './financial-sql.js';

export class Migration202610110002 extends Migration {
  override async up(): Promise<void> { this.addSql(financialUp); }
  override async down(): Promise<void> { this.addSql(financialDown); }
}
